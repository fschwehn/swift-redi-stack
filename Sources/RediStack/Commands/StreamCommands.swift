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
        _ entry: RedisHash,
        to key: String,
        id: String = "*"
    ) -> EventLoopFuture<String> {
        let args: [RESPValue] = [
            .init(bulk: key),
            .init(bulk: id),
        ]
            + entry.flattened()
        
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
    
//    XGROUP
//    XINFO
    
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
    
    
//    XREAD
    // XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
    
    @inlinable
    public func xread<Value: RESPValueConvertible>(
        from streamPositions: [String : String],
        maxCount count: Int? = nil,
        blockFor milliseconds: Int? = nil
    ) -> EventLoopFuture<Value> {
        var args = [RESPValue]()
        
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
    
//    XREADGROUP
//    XREVRANGE
//    XTRIM
    
}
