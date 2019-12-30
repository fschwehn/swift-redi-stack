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
        let stream = "s"
        let group = "g"
        let consumer = "c"
        
        XCTAssertTrue(try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait())
        _ = try connection.xadd(["a":"b"], to: stream).wait()
        _ = try connection.xreadgroup(group: group, consumer: consumer, from: [(stream, ">")]).wait()
        
        let numDeleted = try connection.xgroupDelConsumer(stream, group: group, consumer: consumer).wait()
        XCTAssertEqual(numDeleted, 1)
    }
    
    func test_help() throws {
        XCTAssert(try connection.xgroupHelp().wait().count > 1)
    }

    func test_xread() throws {
        // read empty stream
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
                RedisStreamEntry(id: id1, hash: msg1Hash),
                RedisStreamEntry(id: id2, hash: msg2Hash),
            ]
        ]
        
        XCTAssertEqual(response, expected)
    }
    
    func test_xreadgroup() throws {
        let group0 = "g0"
        let consumer0 = "c0"
        let stream0 = "s0"
        let stream1 = "s1"
        let msg_s0_1: RedisHash = ["0": "1"]
        let msg_s1_1: RedisHash = ["1": "1"]
        let msg_s1_2: RedisHash = ["1": "2"]
        let msg_s1_3: RedisHash = ["1": "3"]
        
        let id_s0_1 = try connection.xadd(msg_s0_1, to: stream0).wait()
        let id_s1_1 = try connection.xadd(msg_s1_1, to: stream1).wait()
        let id_s1_2 = try connection.xadd(msg_s1_2, to: stream1).wait()
        let _ = try connection.xadd(msg_s1_3, to: stream1).wait()
        
        XCTAssertTrue(try connection.xgroupCreate(stream0, group: group0).wait())
        XCTAssertTrue(try connection.xgroupCreate(stream1, group: group0).wait())
        
        let response: RedisXREADResponse = try connection.xreadgroup(group: group0, consumer: consumer0, from: [(stream0, ">"), (stream1, ">")], maxCount: 2).wait()
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
        
        let _: RedisXREADResponse = try connection.xreadgroup(group: group, consumer: consumer, from: [(stream, ">")]).wait()

        XCTAssertEqual(try connection.xack(stream, group: group, ids: [id1, id2]).wait(), 2)
    }
    
    func test_xrange() throws {
        let stream = "s0"
        var messages = [RedisHash]()
        var ids = [String]()
        var results = [RedisStreamEntry]()
        
        for i in 0 ... 3 {
            let msg = RedisHash(dictionaryLiteral: ("a", i))
            let id = try connection.xadd(msg, to: stream).wait()
            messages.append(msg)
            ids.append(id)
            results.append(.init(id: id, hash: msg))
        }
        
        XCTAssertEqual(try connection.xrange(stream, start: ids[0], end: ids[3]).wait(), results)
        XCTAssertEqual(try connection.xrange(stream, start: ids[1], end: ids[2]).wait(), [results[1], results[2]])
        XCTAssertEqual(try connection.xrange(stream, start: ids[1], end: ids[2], reverse: true).wait(), [RedisStreamEntry]())
        XCTAssertEqual(try connection.xrange(stream, start: ids[2], end: ids[1], reverse: true).wait(), [results[2], results[1]])
        XCTAssertEqual(try connection.xrange(stream, start: ids[1], end: ids[3], count: 1).wait(), [results[1]])
    }
    
    func test_xtrim() throws {
        let stream = "s0"
        XCTAssertEqual(try connection.xlen(stream).wait(), 0)
        
        for i in 1 ... 1200 {
            _ = try connection.xadd(RedisHash(dictionaryLiteral: ("a", i)), to: stream).wait()
        }
        
        XCTAssertEqual(try connection.xlen(stream).wait(), 1200)
        XCTAssertTrue(try connection.xtrim(stream, maxLength: 1000).wait() >= 0)
        _ = try connection.xtrim(stream, maxLength: 42, exact: true).wait()
        XCTAssertEqual(try connection.xlen(stream).wait(), 42)
    }

    func test_xinfoStream() throws {
        let stream = "s0"
        
        let msg1: RedisHash = ["a": 1]
        let msg2: RedisHash = ["b": 2]
        let msg3: RedisHash = ["c": 3]
        
        let id1 = try connection.xadd(msg1, to: stream).wait()
        let _   = try connection.xadd(msg2, to: stream).wait()
        let id3 = try connection.xadd(msg3, to: stream).wait()
        
        _ = try connection.xgroupCreate(stream, group: "g1").wait()
        _ = try connection.xgroupCreate(stream, group: "g2").wait()
        
        let info: RedisStreamInfo = try connection.xinfoStream(stream).wait()
        
        XCTAssertEqual(info.length, 3)
        XCTAssertGreaterThan(info.radixTreeKeys, 0)
        XCTAssertGreaterThan(info.radixTreeNodes, 0)
        XCTAssertEqual(info.groups, 2)
        XCTAssertEqual(info.lastGeneratedId, id3)
        XCTAssertEqual(info.firstEntry, RedisStreamEntry(id: id1, hash: msg1))
        XCTAssertEqual(info.lastEntry, RedisStreamEntry(id: id3, hash: msg3))
    }
    
    func test_xinfoGroups() throws {
        let stream = "s0"
        let group0 = "g0"
        let group1 = "g1"
        let consumer0 = "c0"
        
        let msg1: RedisHash = ["a": 1]
        let msg2: RedisHash = ["b": 2]
        let msg3: RedisHash = ["c": 3]

        let id1 = try connection.xadd(msg1, to: stream).wait()
        let _   = try connection.xadd(msg2, to: stream).wait()
        let _ = try connection.xadd(msg3, to: stream).wait()

        _ = try connection.xgroupCreate(stream, group: group0).wait()
        _ = try connection.xgroupCreate(stream, group: group1).wait()
        
        _ = try connection.xreadgroup(group: group0, consumer: consumer0, from: [(stream, ">")], maxCount: 1).wait()

        let infos: [RedisGroupInfo] = try connection.xinfoGroups(stream).wait()

        XCTAssertEqual(infos.count, 2)

        let info0 = infos[0]

        XCTAssertEqual(info0.name, group0)
        XCTAssertEqual(info0.consumers, 1)
        XCTAssertEqual(info0.pending, 1)
        XCTAssertEqual(info0.lastDeliveredId, id1)

        let info1 = infos[1]
        
        XCTAssertEqual(info1.name, group1)
        XCTAssertEqual(info1.consumers, 0)
        XCTAssertEqual(info1.pending, 0)
        XCTAssertEqual(info1.lastDeliveredId, "0-0")
    }
    
    func test_xinfoConsumers() throws {
        let stream = "s0"
        let group = "g0"
        let consumer0 = "c0"
        let consumer1 = "c1"
        
        for i in 1 ... 3 {
            _ = try connection.xadd(["a": i], to: stream).wait()
        }
        
        _ = try connection.xgroupCreate(stream, group: group).wait()

        _ = try connection.xreadgroup(group: group, consumer: consumer0, from: [(stream, ">")], maxCount: 1).wait()
        _ = try connection.xreadgroup(group: group, consumer: consumer1, from: [(stream, ">")]).wait()

        let infos = try connection.xinfoConsumers(stream, group: group).wait()
        
        XCTAssertEqual(infos.count, 2)

        let info0 = infos[0]
        
        XCTAssertEqual(info0.name, consumer0)
        XCTAssertEqual(info0.pending, 1)
        XCTAssertGreaterThanOrEqual(info0.idle, 0)
        
        let info1 = infos[1]

        XCTAssertEqual(info1.name, consumer1)
        XCTAssertEqual(info1.pending, 2)
        XCTAssertGreaterThanOrEqual(info1.idle, 0)
    }
    
    func test_xpending() throws {
        let stream = "s0"
        let group = "g0"
        let consumer0 = "c0"
        let consumer1 = "c1"
        
        for i in 1 ... 4 {
            _ = try connection.xadd(["a": i], to: stream, id: "0-\(i)").wait()
        }
        
        _ = try connection.xgroupCreate(stream, group: group).wait()

        // empty check
        XCTAssertNil(try connection.xpending(stream, group: group).wait())
        XCTAssertEqual((try connection.xpending(stream, group: group, smallestId: "-", greatestId: "+", count: 100).wait()).count, 0)
        
        _ = try connection.xreadgroup(group: group, consumer: consumer0, from: [(stream, ">")], maxCount: 2).wait()
        _ = try connection.xreadgroup(group: group, consumer: consumer1, from: [(stream, ">")], maxCount: 1).wait()
        
        // simple form
        let res1: RedisXPendingSimpleResponse! = try connection.xpending(stream, group: group).wait()
        
        XCTAssertNotNil(res1)
        XCTAssertEqual(res1.pending, 3)
        XCTAssertEqual(res1.smallestPendingId, "0-1")
        XCTAssertEqual(res1.greatestPendingId, "0-3")
        XCTAssertEqual(res1.consumers.count, 2)
        XCTAssertTrue(res1.consumers.contains(.init(name: consumer0, pending: 2)))
        XCTAssertTrue(res1.consumers.contains(.init(name: consumer1, pending: 1)))
        
        // extended form without specific consumer
        let infos_c0_c1 = try connection.xpending(stream, group: group, smallestId: "-", greatestId: "+", count: 100).wait()
        XCTAssertEqual(infos_c0_c1.count, 3)
        
        XCTAssertTrue(infos_c0_c1.contains(where: {
            $0.id == "0-1" && $0.consumer == consumer0 && $0.millisecondsSinceLastDelivered >= 0 && $0.deliveryCount == 1
        }))
        
        XCTAssertTrue(infos_c0_c1.contains(where: {
            $0.id == "0-2" && $0.consumer == consumer0 && $0.millisecondsSinceLastDelivered >= 0 && $0.deliveryCount == 1
        }))
        
        XCTAssertTrue(infos_c0_c1.contains(where: {
            $0.id == "0-3" && $0.consumer == consumer1 && $0.millisecondsSinceLastDelivered >= 0 && $0.deliveryCount == 1
        }))
        
        // extended form with specific consumer
        let infos_c1 = try connection.xpending(stream, group: group, smallestId: "-", greatestId: "+", count: 100, consumer: consumer1).wait()
        XCTAssertEqual(infos_c1.count, 1)
        
        XCTAssertTrue(infos_c1.contains(where: {
            $0.id == "0-3" && $0.consumer == consumer1 && $0.millisecondsSinceLastDelivered >= 0 && $0.deliveryCount == 1
        }))
    }
    
    func test_xclaim() throws {
        let stream = "s"
        let group = "g"
        let consumer0 = "c0"
        let consumer1 = "c1"
        let id0 = "0-1"
        let msg1: RedisHash = ["a": 1]
        
        _ = try connection.xgroupCreate(stream, group: group, createStreamIfNotExists: true).wait()
        
        // test empty response
        XCTAssertEqual((try connection.xclaim(stream, group: group, consumer: consumer0, minIdleTime: 1, ids: [id0]).wait()).count, 0)
        
        // add and consume a message
        let id1 = try connection.xadd(msg1, to: stream).wait()
        _ = try connection.xreadgroup(group: group, consumer: consumer0, from: [(stream, ">")], maxCount: 1).wait()
        
        // assert message is pending
        XCTAssertEqual((try connection.xpending(stream, group: group).wait()?.pending), 1)
        
        // claim pending message and check result
        let res = try connection.xclaim(stream, group: group, consumer: consumer1, minIdleTime: 1, ids: [id1]).wait()
        XCTAssertEqual(res, [RedisStreamEntry(id: id1, hash: msg1)])
    }
    
}
