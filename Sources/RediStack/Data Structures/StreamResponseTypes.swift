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


public struct RedisXPendingSimpleResponse {
    public let pending: Int
    public let smallestPendingId: String
    public let greatestPendingId: String
    public let consumers: [Consumer]
    
    public struct Consumer {
        public let name: String
        public let pending: Int
    }
}

extension RedisXPendingSimpleResponse: Equatable {}
extension RedisXPendingSimpleResponse.Consumer: Equatable {}

extension RedisXPendingSimpleResponse: RESPDecodableToOptional {
    
    public init?(_ value: RESPValue) throws {
        do {
            let arr = try [RESPValue].decode(value)
            
            guard arr.count >= 4 else { throw RESPDecodingError.arrayOutOfBounds }
            
            let pending = try Int.decode(arr[0])
            
            guard pending > 0 else { return nil }
            
            let consumersArr = try [RESPValue].decode(arr[3])
            let consumers: [RedisXPendingSimpleResponse.Consumer] = try consumersArr.map {
                let consumerArr = try [RESPValue].decode($0)
                guard consumerArr.count >= 2 else { throw RESPDecodingError.arrayOutOfBounds }
                
                return RedisXPendingSimpleResponse.Consumer(
                    name: try .decode(consumerArr[0]),
                    pending: try .decode(consumerArr[1])
                )
            }
            
            self.init(
                pending: pending,
                smallestPendingId: try .decode(arr[1]),
                greatestPendingId: try .decode(arr[2]),
                consumers: consumers
            )
        }
        catch {
            throw RESPDecodingError.complex(expectedType: RedisXPendingSimpleResponse.self, value: value, underlyingError: error)
        }
    }
    
}
