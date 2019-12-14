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

//    func test_xadd() throws {
//        let entry: RedisHash = [
//            "name": "joe",
//            "age": "42",
//        ]
//
//        let id = "0-1"
//        let res: String = try connection.xadd(entry, to: "stream", id: id).wait()
//
//        XCTAssertEqual(res, id)
//    }
//
//    func test_xlen() throws {
//        let stream = "strm"
//        _ = try connection.xadd(["a": "42"], to: stream).wait()
//        _ = try connection.xadd(["b": "23"], to: stream).wait()
//        XCTAssertEqual(try connection.xlen(stream).wait(), 2)
//        XCTAssertEqual(try connection.xlen("empty").wait(), 0)
//    }
//
//    func test_xdel() throws {
//        let stream = "strm"
//        let id1 = try connection.xadd(["a": "1"], to: stream).wait()
//        let id2 = try connection.xadd(["b": "2"], to: stream).wait()
//        let count = try connection.xdel(stream, ids: [id1, id2]).wait()
//        XCTAssertEqual(count, 2)
//        XCTAssertEqual(try connection.xdel("empty", ids: [id1]).wait(), 0)
//    }
//
//    func test_xread() throws {
//        // read empty stream
////        XCTAssertEqual(try connection.xread(from: [.init(key: "empty", id: "$")]).wait(), RESPValue.null)
//
//        // read from stream
//        let key = "strm"
//        let id1 = try connection.xadd(["a": "1"], to: key, id: "0-1").wait()
//        _ = try connection.xadd(["a": "2", "b": "x"], to: key, id: "0-2").wait()
//
//        // [[strm,[[0-2,[b,2]]]]]
//        // [[strm,[[0-2,[b,2]]]]]
//
//        typealias Response = [[String: [[String:RedisHash]]]]
//
//        let result: Response = try connection.xread(from: [.init(key: key, id: id1)]).wait()
//        print(result)
//////        guard case let .array(values) = result else { return XCTFail() }
////
//////        let expected = [
//////            KeyedRedisValue(key: <#T##String#>, value: <#T##_#>)
//////        ]
//////
//////        print(result)
//////        XCTAssertEqual(result, expected)
//    }
//
//    func test_foo() throws {
//        let key = "hash"
//        try connection.hmset(["a": 42, "b": "foo"], in: key).wait()
//        let hash = try connection.hgetall(from: key).wait()
//        print(hash)
//    }
//
////    func test_bar() throws {
////        typealias Response = RedisClient.KeyedXREADResponse
////
////        let response: Response = [
////            "stream1": [["0-1": ["a": "1"]]],
////            "stream2": [["1-1": ["b": "1"]]],
////        ]
////
////        let respValue = response.convertedToRESPValue()
////        print(respValue)
////        XCTAssertEqual(response, Response(fromRESP: respValue))
////    }
}
