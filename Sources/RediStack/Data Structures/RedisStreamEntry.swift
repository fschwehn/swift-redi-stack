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

public struct RedisStreamEntry {
    public let id: String
    public let hash: RedisHash
}

extension RedisStreamEntry: RESPDecodable {
    
    public static func decode(_ value: RESPValue) throws -> RedisStreamEntry {
        let arr = try [RESPValue].decode(value)
        
        guard arr.count >= 2 else {
            throw RESPDecodingError.arrayOutOfBounds
        }
        
        return .init(
            id: try .decode(arr[0]),
            hash: try .decode(arr[1])
        )
    }
    
}

extension RedisStreamEntry: Equatable {}
