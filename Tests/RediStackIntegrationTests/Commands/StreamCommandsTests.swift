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
    
    func test_xgroupCreate() throws {
        let stream = "s"
        let group = "g"
        
        XCTAssertThrowsError(try connection.xgroupCreate(stream, group: group).wait(), "Should fail because stream does not exist")
        XCTAssertTrue(try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait())
        XCTAssertThrowsError(try connection.xgroupCreate(stream, group: group).wait(), "Same group should not be created twice")
    }
    
    func test_xgroupSetId() throws {
        let stream = "s"
        let group = "g"
        
        XCTAssertTrue(try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait())
        XCTAssertTrue(try connection.xgroupSetId(stream, group: group, id: "0-1").wait())
    }
    
    func test_xgroupDestroy() throws {
        let stream = "s"
        let group = "g"
        
        XCTAssertTrue(try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait())
        XCTAssertEqual(try connection.xgroupDestroy(stream, group: group).wait(), 1)
        XCTAssertEqual(try connection.xgroupDestroy(stream, group: group).wait(), 0, "Should only return 1 if group did still exist")
    }
    
    func test_xgroupDelConsumer() throws {
        XCTFail("Not sure how to test this")
    }
    
    func test_help() throws {
        XCTAssert(try connection.xgroupHelp().wait().count > 1)
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
    
    func test_xreadgroup() throws {
        let group0 = "g0"
        let consumer0 = "c0"
        let stream0 = "s0"
        let stream1 = "s1"
        let msg_s0_1: RedisHash = ["a": "1"]
        let msg_s1_1: RedisHash = ["b": "2"]
        let msg_s1_2: RedisHash = ["c": "3"]
        
        XCTAssertTrue(try connection.xgroupCreate(stream0, group: group0, createStreamIfNotExists: true).wait())
        XCTAssertTrue(try connection.xgroupCreate(stream1, group: group0, createStreamIfNotExists: true).wait())
        
        let id_s0_1 = try connection.xadd(msg_s0_1, to: stream0).wait()
        let id_s1_1 = try connection.xadd(msg_s1_1, to: stream1).wait()
        let id_s1_2 = try connection.xadd(msg_s1_2, to: stream1).wait()
        
        let response: RedisXREADResponse = try connection.xreadgroup(group: group0, consumer: consumer0, from: [stream0: ">", stream1: ">"]).wait()
        let expected: RedisXREADResponse = [
            stream0: [
                .init(id: id_s0_1, hash: msg_s0_1)
            ],
            stream1: [
                .init(id: id_s1_1, hash: msg_s1_1),
                .init(id: id_s1_2, hash: msg_s1_2),
            ],
        ]
    
        XCTAssertEqual(response, expected)
    }

    func test_xack() throws {
        let group = "g0"
        let consumer = "c0"
        let stream = "s0"
        let msg1: RedisHash = ["a": "1"]
        let msg2: RedisHash = ["b": "2"]
        
        XCTAssertTrue(try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait())

        let id1 = try connection.xadd(msg1, to: stream).wait()
        let id2 = try connection.xadd(msg2, to: stream).wait()
        
        let _: RedisXREADResponse = try connection.xreadgroup(group: group, consumer: consumer, from: [stream: ">"]).wait()
        
        XCTAssertEqual(try connection.xack(stream, group: group, ids: [id1, id2]).wait(), 2)
    }
    
}
