//===----------------------------------------------------------------------===//
//
// This source file is part of the RediStack open source project
//
// Copyright (c) 2019 RediStack project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of RediStack project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import struct Foundation.UUID
import Logging
import NIO

/// The `NIO.ChannelOutboundHandler.OutboundIn` type for `RedisCommandHandler`.
///
/// This holds the full command message to be sent to Redis, and an `NIO.EventLoopPromise` to be fulfilled when a response has been received.
/// - Important: This struct has _reference semantics_ due to the retention of the `NIO.EventLoopPromise`.
public struct RedisCommand {
    /// A message waiting to be sent to Redis. A full message contains a command keyword and its arguments stored as a single `RESPValue.array`.
    public let message: RESPValue
    /// A promise to be fulfilled with the sent message's response from Redis.
    public let responsePromise: EventLoopPromise<RESPValue>

    public init(message: RESPValue, responsePromise promise: EventLoopPromise<RESPValue>) {
        self.message = message
        self.responsePromise = promise
    }
}

public class _RedisCommandHandlerCore {
    /// FIFO queue of promises waiting to receive a response value from a sent command.
    internal var commandResponseQueue: CircularBuffer<EventLoopPromise<RESPValue>>
    internal var logger: Logger
    
    deinit {
        guard self.commandResponseQueue.count > 0 else { return }
        self.logger[metadataKey: "Queue Size"] = "\(self.commandResponseQueue.count)"
        self.logger.warning("Command handler deinit when queue is not empty")
    }
    
    internal init(startingCapacity: Int, logger: Logger) {
        self.commandResponseQueue = CircularBuffer(initialCapacity: startingCapacity)
        self.logger = logger
    }
    
    internal func replace<Other: _RedisCommandHandlerCore>(other handler: Other) {
        guard self.commandResponseQueue.isEmpty else {
            self.logger.critical(
                "Command handler attempted to replace another while the handler was already in use.",
                metadata: [
                    "handlerTypeDoingReplace": "\(String(describing: type(of: self)))",
                    "handlerTypeBeingReplaced": "\(String(describing: Other.self))"
                ]
            )
            preconditionFailure("This method should be called before commands are allowed to be added to this instance's queue!")
        }
        self.commandResponseQueue = handler.commandResponseQueue
    }
    
    internal func handleRedisResponse(_ value: RESPValue) {
        guard let leadPromise = self.commandResponseQueue.popFirst() else {
            assertionFailure("Read triggered with an empty promise queue! Ignoring: \(value)")
            self.logger.critical("Read triggered with no promise waiting in the queue!")
            return
        }
        
        switch value {
        case let .error(e):
            leadPromise.fail(e)
            RedisMetrics.commandFailureCount.increment()
            
        default:
            leadPromise.succeed(value)
            RedisMetrics.commandSuccessCount.increment()
        }
    }
    
    internal func handlePipelineError(
        _ error: Error,
        within context: ChannelHandlerContext,
        cascadingTo promise: EventLoopPromise<Void>? = nil
    ) {
        let queue = self.commandResponseQueue
        
        assert(queue.count > 0, "Received unexpected error while idle: \(error.localizedDescription)")
        
        self.commandResponseQueue.removeAll()
        queue.forEach { $0.fail(error) }
        
        self.logger.critical("Error in channel pipeline.", metadata: ["error": "\(error.localizedDescription)"])
        
        context.close(promise: promise)
    }
}

// all `CommandHandler`s should send out RESPValues
extension _RedisCommandHandlerCore {
    /// See `NIO.ChannelOutboundHandler.OutboundOut`
    public typealias OutboundOut = RESPValue
}

/// An object that operates in a First In, First Out (FIFO) request-response cycle.
///
/// `RedisCommandHandler` is a `NIO.ChannelDuplexHandler` that sends `RedisCommand` instances to Redis,
/// and fulfills the command's `NIO.EventLoopPromise` as soon as a `RESPValue` response has been received from Redis.
public final class RedisCommandHandler: _RedisCommandHandlerCore {
    /// Initializes a new handler to coordinate outgoing requests to, and incoming response from, a Redis instance.
    ///
    /// `RedisCommandHandler` stores all `RedisCommand.responsePromise` objects into a buffer, and unless you intend to execute several concurrent
    /// commands against Redis, and don't want the buffer to resize, you shouldn't need to set `initialQueueCapacity`.
    ///
    /// The `logger` instance, either the given or default one, will have a `Foundation.UUID` value attached as metadata to uniquely identify this instance.
    /// - Parameters:
    ///     - initialQueueCapacity: The initial queue size to start with. The default is `3`.
    ///     - logger: The `Logging.Logger` instance to use. If one is not provided, a default will be created.
    public init(initialQueueCapacity: Int = 3, logger: Logger? = nil) {
        super.init(
            startingCapacity: initialQueueCapacity,
            logger: logger ?? Logger(label: "RediStack.CommandHandler")
        )
        self.logger[metadataKey: "CommandHandler"] = "\(UUID())"
    }
}

// MARK: ChannelInboundHandler

extension RedisCommandHandler: ChannelInboundHandler {
    /// See `NIO.ChannelInboundHandler.InboundIn`
    public typealias InboundIn = RESPValue

    /// Invoked by SwiftNIO when an error has been thrown. The command queue will be drained, with each promise in the queue being failed with the error thrown.
    ///
    /// See `NIO.ChannelInboundHandler.errorCaught(context:error:)`
    /// - Important: This will also close the socket connection to Redis.
    /// - Note:`RedisMetrics.commandFailureCount` is **not** incremented from this error.
    ///
    /// A `Logging.LogLevel.critical` message will be written with the caught error.
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.handlePipelineError(error, within: context)
    }

    /// Invoked by SwiftNIO when a read has been fired from earlier in the response chain.
    ///
    /// This forwards the decoded `RESPValue` response message to the promise waiting to be fulfilled at the front of the command queue.
    /// - Note: `RedisMetrics.commandFailureCount` and `RedisMetrics.commandSuccessCount` are incremented from this method.
    ///
    /// See `NIO.ChannelInboundHandler.channelRead(context:data:)`
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.handleRedisResponse(self.unwrapInboundIn(data))
    }
}

// MARK: ChannelOutboundHandler

extension RedisCommandHandler: ChannelOutboundHandler {
    /// See `NIO.ChannelOutboundHandler.OutboundIn`
    public typealias OutboundIn = RedisCommand

    /// Invoked by SwiftNIO when a `write` has been requested on the `Channel`.
    ///
    /// This unwraps a `RedisCommand`, storing the `NIO.EventLoopPromise` in a command queue,
    /// to fulfill later with the response to the command that is about to be sent through the `NIO.Channel`.
    ///
    /// See `NIO.ChannelOutboundHandler.write(context:data:promise:)`
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let command = self.unwrapOutboundIn(data)
        
        self.commandResponseQueue.append(command.responsePromise)
        
        context.write(self.wrapOutboundOut(command.message), promise: promise)
    }
}
