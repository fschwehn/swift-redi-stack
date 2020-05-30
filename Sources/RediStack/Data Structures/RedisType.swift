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

import Foundation

/// Represents different redis key types
public enum RedisType: String {
    case string
    case list
    case set
    case zset
    case hash
    case stream
    case none
    case unknown
}

extension RedisType: RESPValueConvertible {
    
    public init?(fromRESP value: RESPValue) {
        guard let raw = String(fromRESP: value) else { return nil }
        self = RedisType(rawValue: raw) ?? .unknown
    }
    
    public func convertedToRESPValue() -> RESPValue {
        return .init(bulk: rawValue)
    }
    
}
