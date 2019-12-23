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
import Foundation

extension RedisClient {
    
    @inlinable
    public func xack(_ key: String, group: String, ids: [String]) -> EventLoopFuture<Int> {
        var args: [RESPValue] = [
            .init(bulk:key),
            .init(bulk:group),
        ]
        
        args += ids.map(RESPValue.init)
        
        return send(command: "XACK", with: args)
            .convertFromRESPValue()
    }
    
    ///
    /// Appends the specified stream entry to the stream at the specified key.
    /// [https://redis.io/commands/xadd](https://redis.io/commands/xadd)
    /// - Parameters:
    ///   - entry: The entry
    ///   - key: The stream key
    ///   - id: entry ID
    /// - Returns: The ID of the added entry
    @inlinable
    public func xadd(
        _ message: RedisHash,
        to key: String,
        id: String = "*"
    ) -> EventLoopFuture<String> {
        let args: [RESPValue] = [
            .init(bulk: key),
            .init(bulk: id),
        ]
            + message.flattened()
        
        return send(command: "XADD", with: args)
            .convertFromRESPValue()
    }

    @inlinable
    public func xclaim() {
        
    }
    
    /// Removes the specified entries from a stream.
    /// - Parameter key: The stream's key
    /// - Returns: The number of entries deleted
    @inlinable
    public func xdel<T: Sequence>(_ key: String, ids: T) -> EventLoopFuture<Int> where T.Element == String {
        var args = [RESPValue.init(bulk: key)]
        
        for id in ids {
            args.append(id.convertedToRESPValue())
        }
        
        return send(command: "XDEL", with: args)
            .convertFromRESPValue()
    }
    
    // MARK: Groups
    
    @inlinable
    public func xgroupCreate(_ key: String, group: String, id: String = "0", createStreamIfNotExists: Bool = false) -> EventLoopFuture<Bool> {
        var args: [RESPValue] = [
            .init(bulk:"CREATE"),
            .init(bulk:key),
            .init(bulk:group),
            .init(bulk:id)
        ]
        
        if createStreamIfNotExists {
            args.append(.init(bulk: "MKSTREAM"))
        }
        
        return send(command: "XGROUP", with: args)
            .convertFromRESPValue(to: String.self)
            .map { $0 == "OK" }
    }
    
    @inlinable
    public func xgroupSetId(_ key: String, group: String, id: String) -> EventLoopFuture<Bool> {
        let args: [RESPValue] = [
            .init(bulk:"SETID"),
            .init(bulk:key),
            .init(bulk:group),
            .init(bulk:id)
        ]
        
        return send(command: "XGROUP", with: args)
            .convertFromRESPValue(to: String.self)
            .map { $0 == "OK" }
    }
    
    @inlinable
    public func xgroupDestroy(_ key: String, group: String) -> EventLoopFuture<Int> {
        let args: [RESPValue] = [
            .init(bulk:"DESTROY"),
            .init(bulk:key),
            .init(bulk:group)
        ]
        
        return send(command: "XGROUP", with: args)
            .convertFromRESPValue(to: Int.self)
    }
    
    @inlinable
    public func xgroupDelConsumer(_ key: String, group: String, consumer: String) -> EventLoopFuture<Int> {
        let args: [RESPValue] = [
            .init(bulk:"DELCONSUMER"),
            .init(bulk:key),
            .init(bulk:group),
            .init(bulk:consumer)
        ]
        
        return send(command: "XGROUP", with: args)
            .convertFromRESPValue(to: Int.self)
    }
    
    @inlinable
    public func xgroupHelp() -> EventLoopFuture<[String]> {
        let args: [RESPValue] = [
            .init(bulk:"HELP")
        ]
        
        return send(command: "XGROUP", with: args)
            .convertFromRESPValue()
    }
    
    @inlinable
    public func xinfoStream(_ key: String) -> EventLoopFuture<RedisStreamInfo> {
        let args = [
            RESPValue.init(bulk: "STREAM"),
            RESPValue.init(bulk: key),
        ]
        
        return send(command: "XINFO", with: args)
            .flatMapThrowing(RedisStreamInfo.decode)
    }
    
    @inlinable
    public func xinfoGroups(_ key: String) -> EventLoopFuture<[RedisGroupInfo]> {
        let args = [
            RESPValue.init(bulk: "GROUPS"),
            RESPValue.init(bulk: key),
        ]
        
        return send(command: "XINFO", with: args)
            .flatMapThrowing([RedisGroupInfo].decode)
    }
    
    @inlinable
    public func xinfoConsumers(_ key: String, group: String) -> EventLoopFuture<[RedisConsumerInfo]> {
        let args = [
            RESPValue.init(bulk: "CONSUMERS"),
            RESPValue.init(bulk: key),
            RESPValue.init(bulk: group),
        ]
        
        return send(command: "XINFO", with: args)
            .flatMapThrowing([RedisConsumerInfo].decode)
    }
    
    /// Returns the number of entries inside a stream
    /// - Parameter key: The stream's key
    @inlinable
    public func xlen(_ key: String) -> EventLoopFuture<Int> {
        let args = [RESPValue.init(bulk: key)]
        
        return send(command: "XLEN", with: args)
            .convertFromRESPValue()
    }

    @inlinable
    public func xpending(_ key: String, group: String) -> EventLoopFuture<RedisXPendingSimpleResponse?> {
        let args = [
            RESPValue.init(bulk: key),
            RESPValue.init(bulk: group)
        ]
        
        return send(command: "XPENDING", with: args)
            .flatMapThrowing(RedisXPendingSimpleResponse.decode)
    }
    
    @inlinable
    public func xpending(_ key: String, group: String, smallestId: String, greatestId: String, count: Int) -> EventLoopFuture<[RedisXPendingEntryInfo]> {
        let args: [RESPValue] = [
            .init(bulk: key),
            .init(bulk: group),
            .init(bulk: smallestId),
            .init(bulk: greatestId),
            .init(bulk: count),
        ]
        
        return send(command: "XPENDING", with: args)
            .flatMapThrowing([RedisXPendingEntryInfo].decode)
    }
    
    @inlinable
    public func xrange(
        _ key: String,
        start: String,
        end: String,
        count: Int? = nil,
        reverse: Bool = false
    ) -> EventLoopFuture<[RedisStreamEntry]> {
        var args: [RESPValue] = [
            .init(bulk: key),
            .init(bulk: start),
            .init(bulk: end),
        ]
        
        if let count = count {
            args.append(.init(bulk: "COUNT"))
            args.append(.init(bulk: count))
        }
        
        let command = reverse ? "XREVRANGE" : "XRANGE"
        
        return send(command: command, with: args)
            .flatMapThrowing([RedisStreamEntry].decode)
    }
    
    @inlinable
    public func xrevrange(
        _ key: String,
        start: String,
        end: String,
        count: Int? = nil,
        reverse: Bool = false
    ) -> EventLoopFuture<[RedisStreamEntry]> {
        return xrange(key, start: start, end: end, count: count, reverse: true)
    }
    
    @inlinable
    public func xread<Value: RESPDecodable>(
        from streamPositions: [String : String],
        maxCount count: Int? = nil,
        blockFor milliseconds: Int? = nil
    ) -> EventLoopFuture<Value> {
        var args = [RESPValue]()
        
        args.reserveCapacity(3 + streamPositions.count * 2)
        
        if let count = count {
            args += [
                .init(bulk: "COUNT"),
                .init(bulk: count)
            ]
        }
        
        if let milliseconds = milliseconds {
            args += [
                .init(bulk: "BLOCK"),
                .init(bulk: milliseconds)
            ]
        }
    
        args.append(.init(bulk: "STREAMS"))
        
        for key in streamPositions.keys {
            args.append(.init(bulk: key))
        }
        
        for id in streamPositions.values {
            args.append(.init(bulk: id))
        }
        
        return send(command: "XREAD", with: args)
            .flatMapThrowing(Value.decode)
    }
    
    // @TODO: we want a simplified variant that takes one stream key and one offset instead of a dicationary
    @inlinable
    public func xreadgroup(
        group: String,
        consumer: String,
        from streamPositions: [(String, String)],
        maxCount count: Int? = nil,
        blockFor milliseconds: Int? = nil
    ) -> EventLoopFuture<RedisXREADResponse> {
        var args: [RESPValue] = [
            .init(bulk: "GROUP"),
            .init(bulk: group),
            .init(bulk: consumer),
        ]
        
        args.reserveCapacity(6 + streamPositions.count * 2)
        
        if let count = count {
            args.append(.init(bulk: "COUNT"))
            args.append(.init(bulk: count))
        }
        
        if let milliseconds = milliseconds {
            args.append(.init(bulk: "BLOCK"))
            args.append(.init(bulk: milliseconds))
        }
    
        args.append(.init(bulk: "STREAMS"))
        
        for pos in streamPositions {
            args.append(.init(bulk: pos.0))
        }
        
        for pos in streamPositions {
            args.append(.init(bulk: pos.1))
        }
        
        return send(command: "XREADGROUP", with: args)
            .flatMapThrowing(RedisXREADResponse.decode)
    }
    
    @inlinable
    public func xtrim(
        _ key: String,
        maxLength: Int,
        exact: Bool = false
    ) -> EventLoopFuture<Int> {
        var args = [RESPValue]()
        args.reserveCapacity(4)
        args.append(.init(bulk: key))
        args.append(.init(bulk: "MAXLEN"))
        
        if !exact {
            args.append(.init(bulk: "~"))
        }
        
        args.append(.init(bulk: maxLength))
        
        return send(command: "XTRIM", with: args)
            .convertFromRESPValue()
    }
    
}
