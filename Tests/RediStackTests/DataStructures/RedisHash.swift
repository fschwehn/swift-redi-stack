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

@testable import RediStack
import XCTest

final class RedisHashTests: XCTestCase {

    // @TODO: cleanup
    
    func test_RedisHash() throws {
        let hash: RedisHash = [
            "name": "joe",
            "age": 42,
        ]
        
        let respValue = hash.convertedToRESPValue()
        
        XCTAssertEqual(hash, RedisHash(fromRESP: respValue))
    }
    
    func test_RedisHash_Equality() throws {
        XCTAssertEqual(
            RedisHash(dictionaryLiteral: ("a", "1")),
            RedisHash(dictionaryLiteral: ("a", 1)))
        
        XCTAssertEqual(
            RedisHash(dictionaryLiteral: ("a", ["b": "c"])),
            RedisHash(dictionaryLiteral: ("a", ["b": "c"].description)))
    }
    
}
