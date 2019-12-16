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

public struct RedisXREADResponse: ExpressibleByDictionaryLiteral {
    
    internal var storage = [String : [RedisStreamMessage]]()
    
    public init(dictionaryLiteral elements: (String, [RedisStreamMessage])...) {
        for element in elements {
            storage[element.0] = element.1
        }
    }
    
}

extension RedisXREADResponse: RESPValueConvertible {
    public init?(fromRESP value: RESPValue) {
        switch value {
        case .null:
            break
            
        case .array(let streams):
            storage.reserveCapacity(streams.count)
            
            for stream in streams {
                guard case .array(let list) = stream else { return nil }
                guard list.count == 2 else { return nil }
                guard let key = String(fromRESP: list[0]) else { return nil }
                guard let messages = [RedisStreamMessage](fromRESP: list[1]) else { return nil }
                
                storage[key] = messages
            }
        default:
            return nil
        }
    }

    public func convertedToRESPValue() -> RESPValue {
        return .null
    }
    
    subscript(key: String) -> [RedisStreamMessage]? {
        get {
            return storage[key]
        }
        set(newValue) {
            storage[key] = newValue
        }
    }
}

extension RedisXREADResponse: Equatable {}
