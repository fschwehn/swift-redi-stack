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

public struct RedisXREADResponse {
    
    internal var storage = [String : [RedisStreamEntry]]()
    
    subscript(key: String) -> [RedisStreamEntry]? {
        get {
            return storage[key]
        }
        set(newValue) {
            storage[key] = newValue
        }
    }
    
}

extension RedisXREADResponse: ExpressibleByDictionaryLiteral {
    
    public init(dictionaryLiteral elements: (String, [RedisStreamEntry])...) {
        for element in elements {
            storage[element.0] = element.1
        }
    }
    
}

extension RedisXREADResponse: RESPDecodable {
    
    public static func decode(_ value: RESPValue) throws -> RedisXREADResponse {
        var storage = [String : [RedisStreamEntry]]()
        
        if case .array(let streams) = value {
            for stream in streams {
                let values = try [RESPValue].decode(stream)
                
                guard values.count >= 2 else {
                    throw RESPDecodingError.arrayOutOfBounds
                }
                
                let key = try String.decode(values[0])
                let entries = try [RedisStreamEntry].decode(values[1])
                
                storage[key] = entries
            }
        }
        
        return .init(storage: storage)
    }
    
}

extension RedisXREADResponse: Equatable {}
