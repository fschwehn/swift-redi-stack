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
import RediStackTestUtils
import XCTest

final class StreamCommandsTests: RediStackIntegrationTestCase {
    private static let testKey = "StreamCommandsTests"

    func test_xadd() throws {
        let entry: RedisHash = [
            "name": "joe",
            "age": "42",
        ]

        let id = "0-1"
        let res: String = try connection.xadd(entry, to: "stream", id: id).wait()
        
        XCTAssertEqual(res, id)
    }

    func test_xlen() throws {
        let stream = "strm"
        _ = try connection.xadd(["a": "42"], to: stream).wait()
        _ = try connection.xadd(["b": "23"], to: stream).wait()
        XCTAssertEqual(try connection.xlen(stream).wait(), 2)
        XCTAssertEqual(try connection.xlen("empty").wait(), 0)
    }

    func test_xdel() throws {
        let stream = "strm"
        let id1 = try connection.xadd(["a": "1"], to: stream).wait()
        let id2 = try connection.xadd(["b": "2"], to: stream).wait()
        let count = try connection.xdel(stream, ids: [id1, id2]).wait()
        XCTAssertEqual(count, 2)
        XCTAssertEqual(try connection.xdel("empty", ids: [id1]).wait(), 0)
    }

    func test_xread() throws {
        
        // read empty stream
        XCTAssertEqual(try connection.xread(from: ["empty": "$"]).wait(), RESPValue.null)
        XCTAssertEqual(try connection.xread(from: ["empty": "$"]).wait(), RedisXREADResponse())
        
        // read filled stream
        let stream = "strm"
        let id0 = "0"
        let id1 = "0-1"
        let msg1Hash: RedisHash = ["a": 1]
        let id2 = "0-2"
        let msg2Hash: RedisHash = ["b": "foo"]
        
        _ = try connection.xadd(msg1Hash, to: stream, id: id1).wait()
        _ = try connection.xadd(msg2Hash, to: stream, id: id2).wait()
        
        let response: RedisXREADResponse = try connection.xread(from: [stream: id0]).wait()
        let expected: RedisXREADResponse = [
            stream: [
                RedisStreamMessage(id: id1, hash: msg1Hash),
                RedisStreamMessage(id: id2, hash: msg2Hash),
            ]
        ]
        
        XCTAssertEqual(response, expected)
    }
    
}
