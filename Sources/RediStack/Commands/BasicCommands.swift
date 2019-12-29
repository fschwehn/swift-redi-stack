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
    /// Echos the provided message through the Redis instance.
    ///
    /// See [https://redis.io/commands/echo](https://redis.io/commands/echo)
    /// - Parameter message: The message to echo.
    /// - Returns: The message sent with the command.
    @inlinable
    public func echo(_ message: String) -> EventLoopFuture<String> {
        let args = [RESPValue(bulk: message)]
        return send(command: "ECHO", with: args)
            .convertFromRESPValue()
    }

    /// Pings the server, which will respond with a message.
    ///
    /// See [https://redis.io/commands/ping](https://redis.io/commands/ping)
    /// - Parameter message: The optional message that the server should respond with.
    /// - Returns: The provided message or Redis' default response of `"PONG"`.
    @inlinable
    public func ping(with message: String? = nil) -> EventLoopFuture<String> {
        let args: [RESPValue] = message != nil
            ? [.init(bulk: message!)] // safe because we did a nil pre-check
            : []
        return send(command: "PING", with: args)
            .convertFromRESPValue()
    }

    /// Select the Redis logical database having the specified zero-based numeric index.
    /// - Note: New connections always use the database `0`.
    ///
    /// [https://redis.io/commands/select](https://redis.io/commands/select)
    /// - Parameter index: The 0-based index of the database that will receive later commands.
    /// - Returns: An `EventLoopFuture` that resolves when the operation has succeeded, or fails with a `RedisError`.
    @inlinable
    public func select(database index: Int) -> EventLoopFuture<Void> {
        let args = [RESPValue(bulk: index)]
        return send(command: "SELECT", with: args)
            .map { _ in return () }
    }

    /// Swaps the data of two Redis databases by their index IDs.
    ///
    /// See [https://redis.io/commands/swapdb](https://redis.io/commands/swapdb)
    /// - Parameters:
    ///     - first: The index of the first database.
    ///     - second: The index of the second database.
    /// - Returns: `true` if the swap was successful.
    @inlinable
    public func swapDatabase(_ first: Int, with second: Int) -> EventLoopFuture<Bool> {
        let args: [RESPValue] = [
            .init(bulk: first),
            .init(bulk: second)
        ]
        return send(command: "SWAPDB", with: args)
            .convertFromRESPValue(to: String.self)
            .map { return $0 == "OK" }
    }

    /// Removes the specified keys. A key is ignored if it does not exist.
    ///
    /// [https://redis.io/commands/del](https://redis.io/commands/del)
    /// - Parameter keys: A list of keys to delete from the database.
    /// - Returns: The number of keys deleted from the database.
    @inlinable
    public func delete(_ keys: [String]) -> EventLoopFuture<Int> {
        guard keys.count > 0 else { return self.eventLoop.makeSucceededFuture(0) }
        
        let args = keys.map(RESPValue.init)
        return send(command: "DEL", with: args)
            .convertFromRESPValue()
    }
    
    /// Removes the specified keys. A key is ignored if it does not exist.
    ///
    /// [https://redis.io/commands/del](https://redis.io/commands/del)
    /// - Parameter keys: A list of keys to delete from the database.
    /// - Returns: The number of keys deleted from the database.
    @inlinable
    public func delete(_ keys: String...) -> EventLoopFuture<Int> {
        return self.delete(keys)
    }

    /// Sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
    /// - Note: A key with an associated timeout is often said to be "volatile" in Redis terminology.
    ///
    /// [https://redis.io/commands/expire](https://redis.io/commands/expire)
    /// - Parameters:
    ///     - key: The key to set the expiration on.
    ///     - timeout: The time from now the key will expire at.
    /// - Returns: `true` if the expiration was set.
    @inlinable
    public func expire(_ key: String, after timeout: TimeAmount) -> EventLoopFuture<Bool> {
        let amount = timeout.nanoseconds / 1_000_000_000
        let args: [RESPValue] = [
            .init(bulk: key),
            .init(bulk: amount.description)
        ]
        return send(command: "EXPIRE", with: args)
            .convertFromRESPValue(to: Int.self)
            .map { return $0 == 1 }
    }
    
    /// Returns the `RedisType` of the value stored at key.
    /// If a key does not exist `RedisType.none` will be returned.
    /// - Parameter key:The key to get the type for
    @inlinable
    public func type(_ key: String) -> EventLoopFuture<RedisType> {
        let args = [RESPValue(bulk: key)]
        return send(command: "TYPE", with: args)
            .convertFromRESPValue()
    }
}

// MARK: Scan

extension RedisClient {
    /// Incrementally iterates over all keys in the currently selected database.
    ///
    /// [https://redis.io/commands/scan](https://redis.io/commands/scan)
    /// - Parameters:
    ///     - position: The cursor position to start from.
    ///     - count: The number of elements to advance by. Redis default is 10.
    ///     - match: A glob-style pattern to filter values to be selected from the result set.
    /// - Returns: A cursor position for additional invocations with a limited collection of keys found in the database.
    @inlinable
    public func scan(
        startingFrom position: Int = 0,
        count: Int? = nil,
        matching match: String? = nil
    ) -> EventLoopFuture<(Int, [String])> {
        return _scan(command: "SCAN", nil, position, count, match)
    }

    @usableFromInline
    internal func _scan<T>(
        command: String,
        resultType: T.Type = T.self,
        _ key: String?,
        _ pos: Int,
        _ count: Int?,
        _ match: String?
    ) -> EventLoopFuture<(Int, T)>
        where
        T: RESPValueConvertible
    {
        var args: [RESPValue] = [.init(bulk: pos)]

        if let k = key {
            args.insert(.init(bulk: k), at: 0)
        }

        if let m = match {
            args.append(.init(bulk: "match"))
            args.append(.init(bulk: m))
        }
        if let c = count {
            args.append(.init(bulk: "count"))
            args.append(.init(bulk: c))
        }

        let response = send(command: command, with: args).convertFromRESPValue(to: [RESPValue].self)
        let position = response.flatMapThrowing { result -> Int in
            guard
                let value = result[0].string,
                let position = Int(value)
            else {
                throw RedisClientError.assertionFailure(message: "Unexpected value in response: \(result[0])")
            }
            return position
        }
        let elements = response
            .map { return $0[1] }
            .convertFromRESPValue(to: resultType)

        return position.and(elements)
    }
}
