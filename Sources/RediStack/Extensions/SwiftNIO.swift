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

import NIO

extension TimeAmount {
    /// The seconds representation of the TimeAmount.
    @usableFromInline
    internal var seconds: Int64 {
        return self.nanoseconds / 1_000_000_000
    }
}

// MARK: Setting up a Redis connection

extension Channel {
    /// Adds the baseline `ChannelHandlers` needed to support sending and receiving messages in Redis Serialization Protocol (RESP) format to the pipeline.
    ///
    /// For implementation details, see `RedisMessageEncoder`, `RedisByteDecoder`, and `RedisCommandHandler`.
    /// - Returns: An `EventLoopFuture` that resolves after all handlers have been added to the pipeline.
    public func addBaseRedisHandlers() -> EventLoopFuture<Void> {
        let handlers: [(ChannelHandler, name: String)] = [
            (MessageToByteHandler(RedisMessageEncoder()), "RediStack.OutgoingHandler"),
            (ByteToMessageHandler(RedisByteDecoder()), "RediStack.IncomingHandler"),
            (RedisCommandHandler(), "RediStack.CommandHandler")
        ]
        return .andAllSucceed(
            handlers.map { self.pipeline.addHandler($0, name: $1) },
            on: self.eventLoop
        )
    }
}
extension ClientBootstrap {
    /// Makes a new `ClientBootstrap` instance with a baseline Redis `Channel` pipeline
    /// for sending and receiving messages in Redis Serialization Protocol (RESP) format.
    ///
    /// For implementation details, see `RedisMessageEncoder`, `RedisByteDecoder`, and `RedisCommandHandler`.
    /// - Parameter group: The `EventLoopGroup` to create the `ClientBootstrap` with.
    /// - Returns: A `ClientBootstrap` with the base configuration of a `Channel` pipeline for RESP messages.
    public static func makeRedisTCPClient(group: EventLoopGroup) -> ClientBootstrap {
        return ClientBootstrap(group: group)
            .channelOption(
                ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
                value: 1
            )
            .channelInitializer { $0.addBaseRedisHandlers() }
    }
}

// MARK: EventLoopFuture RESPDecodable convenience

extension EventLoopFuture where Value == RESPValue {
    /// Attempts to decode the `RESPValue` to the desired `RESPDecodable` type.
    /// If the `RESPDcodable.decode(_:)` fails, then the `EventLoopFuture` will fail with the corresponding error.
    /// - Parameter to: The desired type to decode to.
    /// - Returns: An `EventLoopFuture` that resolves a value of the desired type.
    @inlinable
    public func decodeFromRESPValue<T>(
        to type: T.Type = T.self,
        file: StaticString = #function,
        function: StaticString = #function,
        line: UInt = #line
    )
        -> EventLoopFuture<T> where T: RESPDecodable
    {
        return self.flatMapThrowing {
            return try T.decode($0)
        }
    }
}
