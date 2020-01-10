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


public struct RedisHash: ExpressibleByDictionaryLiteral {
    
    internal var storage = [String : String]()
    
    public init(dictionaryLiteral elements: (String, CustomStringConvertible)...) {
        for element in elements {
            storage[element.0] = element.1.description
        }
    }
    
    public subscript(key: String) -> String? {
        get {
            return storage[key]
        }
        set(newValue) {
            storage[key] = newValue
        }
    }
    
    public func flattened() -> [RESPValue] {
        var list = [RESPValue](initialCapacity: storage.count * 2)

        for (key, value) in storage {
            list.append(.init(bulk: key))
            list.append(.init(bulk: value))
        }

        return list
    }
    
}

extension RedisHash: RESPValueConvertible {
    public init?(fromRESP value: RESPValue) {
        guard case .array(let list) = value else { return nil }
        guard list.count % 2 == 0 else { return nil }
        
        for fieldIndex in stride(from: 0, to: list.count, by: 2) {
            guard let field = String(fromRESP: list[fieldIndex]) else { return nil }
            guard let value = String(fromRESP: list[fieldIndex + 1]) else { return nil }
            
            storage[field] = value
        }
    }

    public func convertedToRESPValue() -> RESPValue {
        var list = [RESPValue](initialCapacity: storage.count * 2)
        
        for (key, value) in storage {
            list.append(.init(bulk: key))
            list.append(.init(bulk: value))
        }
        
        return .array(list)
    }
}

extension RedisHash: Equatable {}
