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

extension RedisStreamEntry: RESPValueConvertible {
    public init?(fromRESP value: RESPValue) {
        guard case .array(let list) = value else { return nil }
        guard list.count == 2 else { return nil }
        guard let id = String(fromRESP: list[0]) else { return nil }
        guard let hash = RedisHash(fromRESP: list[1]) else { return nil }
        
        self.id = id
        self.hash = hash
    }

    public func convertedToRESPValue() -> RESPValue {
        return .array([
            .init(bulk: id),
            hash.convertedToRESPValue()
        ])
    }
}

extension RedisStreamEntry: Equatable {}
