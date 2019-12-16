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

extension RedisClient {
    
    //    XACK
    
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

//    XCLAIM
    
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
    
    /// Returns the number of entries inside a stream
    /// - Parameter key: The stream's key
    @inlinable
    public func xlen(_ key: String) -> EventLoopFuture<Int> {
        let args = [RESPValue.init(bulk: key)]
        
        return send(command: "XLEN", with: args)
            .convertFromRESPValue()
    }
    
//    XPENDING
//    XRANGE
    
    @inlinable
    public func xread<Value: RESPValueConvertible>(
        from streamPositions: [String : String],
        maxCount count: Int? = nil,
        blockFor milliseconds: Int? = nil
    ) -> EventLoopFuture<Value> {
        var args = [RESPValue]()
        
        args.reserveCapacity(3 + streamPositions.count * 2)
        
        if let count = count {
            args += [.init(bulk: "COUNT"), .integer(count)]
        }
        
        if let milliseconds = milliseconds {
            args += [.init(bulk: "BLOCK"), .integer(milliseconds)]
        }
    
        args.append(.init(bulk: "STREAMS"))
        
        for key in streamPositions.keys {
            args.append(.init(bulk: key))
        }
        
        for id in streamPositions.values {
            args.append(.init(bulk: id))
        }
        
        return send(command: "XREAD", with: args)
            .convertFromRESPValue()
    }
    
    @inlinable
    public func xreadgroup<Value: RESPValueConvertible>(
        group: String,
        consumer: String,
        from streamPositions: [String : String],
        maxCount count: Int? = nil,
        blockFor milliseconds: Int? = nil
    ) -> EventLoopFuture<Value> {
        var args: [RESPValue] = [
            .init(bulk: "GROUP"),
            .init(bulk: group),
            .init(bulk: consumer),
        ]
        
        args.reserveCapacity(6 + streamPositions.count * 2)
        
        if let count = count {
            args += [.init(bulk: "COUNT"), .integer(count)]
        }
        
        if let milliseconds = milliseconds {
            args += [.init(bulk: "BLOCK"), .integer(milliseconds)]
        }
    
        args.append(.init(bulk: "STREAMS"))
        
        for key in streamPositions.keys {
            args.append(.init(bulk: key))
        }
        
        for id in streamPositions.values {
            args.append(.init(bulk: id))
        }
        
        return send(command: "XREADGROUP", with: args)
            .convertFromRESPValue()
    }
    
//    XREVRANGE
//    XTRIM
    
}
