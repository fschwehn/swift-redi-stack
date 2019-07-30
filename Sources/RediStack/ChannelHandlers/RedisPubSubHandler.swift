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
import NIO
import Logging

/// A PubSub subscription callback handler for PubSub messages.
/// - Parameters:
///     - channel: The PubSub channel name that published the message.
///     - message: The published message's data.
public typealias RedisPubSubMessageCallback = (_ channel: String, _ message: RESPValue) -> Void

public final class RedisPubSubHandler: _RedisCommandHandlerCore {
    private var callbackMap: [String: [RedisPubSubMessageCallback]] = [:]
    
    /// Initializes a new handler to register callbacks for PubSub subscriptions in addition to coordinating specific whitelisted commands being sent to a Redis instance.
    ///
    /// `RedisPubSubHandler` stores most `RedisCommand.responsePromise` objects into a buffer.
    /// Unless you intend to execute concurrently several of the whitelisted commands against Redis, and don't want the buffer to resize, you shouldn't need to set `initialQueueCapacity`.
    ///
    /// The `logger` instance, either the given or default one, will have a `Foundation.UUID` value attached as metadata to uniquely identify this instance.
    /// - Parameters:
    ///     - initialQueueCapacity: The initial queue size to start with. The default is `3`.
    ///     - logger: The `Logging.Logger` instance to use. If one is not provided, a default will be created.
    public init(initialQueueCapacity: Int = 1, logger: Logger? = nil) {
        super.init(
            startingCapacity: initialQueueCapacity,
            logger: logger ?? Logger(label: "RediStack.PubSubHandler")
        )
        self.logger[metadataKey: "PubSubHandler"] = "\(UUID())"
    }
    
    /// Initializes a new handler in a state that is ready to fully replace the other handler in a `NIO.Channel`.
    ///
    /// The `logger` instance, either the given or default one, will have a `Foundation.UUID` value attached as metadata to uniquely identify this instance.
    /// - Parameters:
    ///     - handler: The command handler being replaced.
    ///     - logger: The `Logging.Logger` instance to use. If one is not provided, a default will be created.
    convenience public init(replacing handler: RedisCommandHandler, logger: Logger? = nil) {
        self.init(initialQueueCapacity: handler.commandResponseQueue.count, logger: logger)
        self.replace(other: handler)
    }
}

// MARK: ChannelInboundHandler

extension RedisPubSubHandler: ChannelInboundHandler {
    /// The incoming message from Redis that potentially was published to a channel.
    /// It could also be a response to a whitelisted command while in PubSub mode.
    ///
    /// See `NIO.ChannelInboundHandler.InboundIn` and [https://redis.io/topics/pubsub](https://redis.io/topics/pubsub)
    public typealias InboundIn = RESPValue
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let value = self.unwrapInboundIn(data)
        
        #warning("TODO: Check if we get an error")
        
        // check to see if this is our expected PubSub message format ([type, channel, data])
        guard
            let array = value.array, array.count == 3,
            let messageType = PubSubMessageType(fromRESPValue: array[0]),
            let channel = array[1].string
        else {
            self.logger.debug(
                "Received non-PubSub message, treating it as a response to a standard command.",
                metadata: ["value": "\(value)"]
            )
            self.handleRedisResponse(value)
            return
        }
        
        guard
            let subscriptionCount = self.handlePubSubMessage(array[2], from: channel, type: messageType),
            subscriptionCount == 0 // if there are still active subscriptions, we don't need to do anything else
        else { return }
        
        #warning("TODO: Close PubSub mode")
    }
    
    private func handlePubSubMessage(_ message: RESPValue, from channel: String, type: PubSubMessageType) -> Int? {
        switch type {
        // we're just changing subscriptions, so update our subscription count
        case .subscriptionChange:
            guard let subCount = message.int else {
                self.logger.critical(
                    "Reached state where expected PubSub message with a subscription count, but it was missing.",
                    metadata: ["channel": "\(channel)", "message": "\(message)"]
                )
                preconditionFailure("Failed to get the subscription count from a PubSub response.")
            }
            return subCount
            
        // we have an actual message to send to the subscribers,
        // so pass it along to all the callbacks registered to the channel name / pattern filter
        case .message:
            self.callbackMap[channel]?.forEach { $0(channel, message) }
            return nil
        }
    }
    
    private enum PubSubMessageType {
        case subscriptionChange
        case message
        
        init?(fromRESPValue value: RESPValue) {
            switch value.string {
            case "subscribe", "psubscribe", "unsubscribe", "punsubscribe": self = .subscriptionChange
            case "message", "pmessage": self = .message
            default: return nil
            }
        }
    }
}

// MARK: ChannelOutboundHandler

/// A representation of a Redis PubSub command message to be sent to Redis.
/// - Important: `.subscribe` and `.psubscribe` potentially have _reference semantics_ as the callback is an escaping closure.
///
/// See [https://redis.io/topics/pubsub](https://redis.io/topics/pubsub)
public enum RedisPubSubCommand {
    /// See [https://redis.io/commands/subscribe](https://redis.io/commands/subscribe) and [https://redis.io/commands/psubscribe](https://redis.io/commands/psubscribe)
    case subscription(RedisCommand, keys: Set<String>, callback: RedisPubSubMessageCallback)
    /// See [https://redis.io/commands/unsubscribe](https://redis.io/commands/unsubscribe) and [https://redis.io/commands/punsubscribe](https://redis.io/commands/punsubscribe)
    case unsubscribe(RedisCommand, keys: Set<String>)
    /// See [https://redis.io/commands/quit](https://redis.io/commands/quit)
    /// and [https://redis.io/commands/ping](https://redis.io/commands/ping)
    case other(RedisCommand)
    
    /// The underlying `RedisCommand` that this PubSub command represents.
    public var redisCommand: RedisCommand {
        switch self {
        case let .subscription(command, _, _),
             let .unsubscribe(command, _),
             let .other(command):
            return command
        }
    }
}

extension RedisPubSubHandler: ChannelOutboundHandler {
    /// The incoming PubSub command to handle.
    ///
    /// See `NIO.ChannelOutboundHandler.OutboundIn`
    public typealias OutboundIn = RedisPubSubCommand
    
    /// Invoked by SwiftNIO when a `write` has been requested on the `Channel`.
    ///
    /// This unwraps a `RedisPubSubCommand`, to do additional handling before writing the command into the `NIO.Channel`.
    /// If the command is an `.other`, the stored `RedisCommand.responsePromise` is stored in a response queue to to fulfill later with the response from Redis.
    /// `.subscription` and `.unsubscribe` have their related callbacks added or removed from the callback storage.
    ///
    /// See `NIO.ChannelOutboundHandler.write(context:data:promise:)`
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let outgoing = self.unwrapOutboundIn(data)
        let command = outgoing.redisCommand
        
        // for actual PubSub commands (SUBSCRIBE, UNSUBSCRIBE, etc.) we don't need to store the `responsePromise` as
        // the responses come back as the special PubSub format, so we just mimic an "OK" response from Redis and resolve
        let resolvePromise = {
            let response = "OK" // mimics a standard notification response from Redis
            var buffer = context.channel.allocator.buffer(capacity: response.count)
            buffer.writeString(response)
            command.responsePromise.succeed(RESPValue.simpleString(buffer))
        }
        
        #warning("TODO: If subscribing to the exact same pattern / channel isn't allowed, probably want to handle it here")
        switch outgoing {
        case let .subscription(_, keys, callback):
            keys.forEach { key in
                if self.callbackMap.keys.contains(key) {
                    // we've just asserted that it contains it
                    self.callbackMap[key]!.append(callback)
                } else {
                    self.callbackMap[key] = [callback]
                }
            }
            resolvePromise()
            
        case let .unsubscribe(_, keys):
            keys.forEach { self.callbackMap.removeValue(forKey: $0) }
            resolvePromise()
            
        case .other:
            self.commandResponseQueue.append(command.responsePromise)
        }
        
        context.write(self.wrapOutboundOut(command.message), promise: promise)
    }
}
