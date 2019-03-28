@testable import NIORedis
import XCTest

final class SortedSetCommandsTests: XCTestCase {
    private static let testKey = "SortedSetCommandsTests"

    private let redis = RedisDriver(ownershipModel: .internal(threadCount: 1))
    deinit { try? redis.terminate() }

    private var connection: RedisConnection!
    private var key: String { return SortedSetCommandsTests.testKey }

    override func setUp() {
        do {
            connection = try redis.makeConnection().wait()

            var dataset: [(RESPValueConvertible, Double)] = []
            for index in 1...10 {
                dataset.append((index, Double(index)))
            }

            _ = try connection.zadd(dataset, to: SortedSetCommandsTests.testKey).wait()
        } catch {
            XCTFail("Failed to create RedisConnection!")
        }
    }

    override func tearDown() {
        _ = try? connection.send(command: "FLUSHALL").wait()
        connection.close()
        connection = nil
    }

    func test_zadd() throws {
        _ = try connection.send(command: "FLUSHALL").wait()

        XCTAssertThrowsError(try connection.zadd([(30, 2)], to: #function, options: ["INCR"]).wait())

        var count = try connection.zadd([(30, 2)], to: #function).wait()
        XCTAssertEqual(count, 1)
        count = try connection.zadd([(30, 5)], to: #function).wait()
        XCTAssertEqual(count, 0)
        count = try connection.zadd([(30, 6), (31, 0), (32, 1)], to: #function, options: ["NX"]).wait()
        XCTAssertEqual(count, 2)
        count = try connection.zadd([(32, 2), (33, 3)], to: #function, options: ["XX", "CH"]).wait()
        XCTAssertEqual(count, 1)

        var success = try connection.zadd((30, 7), to: #function, options: ["CH"]).wait()
        XCTAssertTrue(success)
        success = try connection.zadd((30, 8), to: #function, options: ["NX"]).wait()
        XCTAssertFalse(success)
    }

    func test_zcard() throws {
        var count = try connection.zcard(of: key).wait()
        XCTAssertEqual(count, 10)

        _ = try connection.zadd(("foo", 0), to: key).wait()

        count = try connection.zcard(of: key).wait()
        XCTAssertEqual(count, 11)
    }

    func test_zscore() throws {
        _ = try connection.send(command: "FLUSHALL").wait()

        var score = try connection.zscore(of: 30, in: #function).wait()
        XCTAssertEqual(score, nil)

        _ = try connection.zadd((30, 1), to: #function).wait()

        score = try connection.zscore(of: 30, in: #function).wait()
        XCTAssertEqual(score, 1)

        _ = try connection.zincrby(10, element: 30, in: #function).wait()

        score = try connection.zscore(of: 30, in: #function).wait()
        XCTAssertEqual(score, 11)
    }

    func test_zscan() throws {
        var (cursor, results) = try connection.zscan(key, count: 5).wait()
        XCTAssertGreaterThanOrEqual(cursor, 0)
        XCTAssertGreaterThanOrEqual(results.count, 5)

        (_, results) = try connection.zscan(key, startingFrom: cursor, count: 8).wait()
        XCTAssertGreaterThanOrEqual(results.count, 8)

        (cursor, results) = try connection.zscan(key, matching: "1*").wait()
        XCTAssertEqual(cursor, 0)
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].1, 1)

        (cursor, results) = try connection.zscan(key, matching: "*0").wait()
        XCTAssertEqual(cursor, 0)
        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results[0].1, 10)
    }

    func test_zrank() throws {
        let futures = [
            connection.zrank(of: 1, in: key),
            connection.zrank(of: 2, in: key),
            connection.zrank(of: 3, in: key),
        ]
        let scores = try EventLoopFuture<Int?>.whenAllSucceed(futures, on: connection.eventLoop).wait()
        XCTAssertEqual(scores, [0, 1, 2])
    }

    func test_zrevrank() throws {
        let futures = [
            connection.zrevrank(of: 1, in: key),
            connection.zrevrank(of: 2, in: key),
            connection.zrevrank(of: 3, in: key),
        ]
        let scores = try EventLoopFuture<Int?>.whenAllSucceed(futures, on: connection.eventLoop).wait()
        XCTAssertEqual(scores, [9, 8, 7])
    }

    func test_zcount() throws {
        var count = try connection.zcount(of: key, within: ("1", "3")).wait()
        XCTAssertEqual(count, 3)
        count = try connection.zcount(of: key, within: ("(1", "(3")).wait()
        XCTAssertEqual(count, 1)
    }

    func test_zlexcount() throws {
        var count = try connection.zlexcount(of: key, within: ("[1", "[3")).wait()
        XCTAssertEqual(count, 3)
        count = try connection.zlexcount(of: key, within: ("(1", "(3")).wait()
        XCTAssertEqual(count, 1)
    }

    func test_zpopmin() throws {
        let min = try connection.zpopmin(from: key).wait()
        XCTAssertEqual(min?.1, 1)

        _ = try connection.zpopmin(from: key, max: 7).wait()

        let results = try connection.zpopmin(from: key, max: 3).wait()
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].1, 9)
        XCTAssertEqual(results[1].1, 10)
    }

    func test_zpopmax() throws {
        let min = try connection.zpopmax(from: key).wait()
        XCTAssertEqual(min?.1, 10)

        _ = try connection.zpopmax(from: key, max: 7).wait()

        let results = try connection.zpopmax(from: key, max: 3).wait()
        XCTAssertEqual(results.count, 2)
        XCTAssertEqual(results[0].1, 2)
        XCTAssertEqual(results[1].1, 1)
    }

    func test_zincrby() throws {
        var score = try connection.zincrby(3_00_1398.328923, element: 1, in: key).wait()
        XCTAssertEqual(score, 3_001_399.328923)

        score = try connection.zincrby(-201_309.1397318, element: 1, in: key).wait()
        XCTAssertEqual(score, 2_800_090.1891912)

        score = try connection.zincrby(20, element: 1, in: key).wait()
        XCTAssertEqual(score, 2_800_110.1891912)
    }

    func test_zunionstore() throws {
        _ = try connection.zadd([(1, 1), (2, 2)], to: #function).wait()
        _ = try connection.zadd([(3, 3), (4, 4)], to: #file).wait()

        let unionCount = try connection.zunionstore(
            as: #function+#file,
            sources: [key, #function, #file],
            weights: [3, 2, 1],
            aggregateMethod: "MAX"
        ).wait()
        XCTAssertEqual(unionCount, 10)
        let rank = try connection.zrank(of: 10, in: #function+#file).wait()
        XCTAssertEqual(rank, 9)
        let score = try connection.zscore(of: 10, in: #function+#file).wait()
        XCTAssertEqual(score, 30)
    }

    func test_zinterstore() throws {
        _ = try connection.zadd([(3, 3), (10, 10), (11, 11)], to: #function).wait()

        let unionCount = try connection.zinterstore(
            as: #file,
            sources: [key, #function],
            weights: [3, 2],
            aggregateMethod: "MIN"
        ).wait()
        XCTAssertEqual(unionCount, 2)
        let rank = try connection.zrank(of: 10, in: #file).wait()
        XCTAssertEqual(rank, 1)
        let score = try connection.zscore(of: 10, in: #file).wait()
        XCTAssertEqual(score, 20.0)
    }

    func test_zrange() throws {
        var elements = try connection.zrange(within: (1, 3), from: key).wait()
        XCTAssertEqual(elements.count, 3)
        elements = try connection.zrange(within: (1, 3), from: key, withScores: true).wait()
        XCTAssertEqual(elements.count, 6)

        let values = try RedisConnection._mapSortedSetResponse(elements, scoreIsFirst: false)
            .map { (value, _) in return Int(value) }

        XCTAssertEqual(values[0], 2)
        XCTAssertEqual(values[1], 3)
        XCTAssertEqual(values[2], 4)
    }

    func test_zrevrange() throws {
        var elements = try connection.zrevrange(within: (1, 3), from: key).wait()
        XCTAssertEqual(elements.count, 3)
        elements = try connection.zrevrange(within: (1, 3), from: key, withScores: true).wait()
        XCTAssertEqual(elements.count, 6)

        let values = try RedisConnection._mapSortedSetResponse(elements, scoreIsFirst: false)
            .map { (value, _) in return Int(value) }

        XCTAssertEqual(values[0], 9)
        XCTAssertEqual(values[1], 8)
        XCTAssertEqual(values[2], 7)
    }

    func test_zrangebyscore() throws {
        var elements = try connection.zrangebyscore(within: ("(1", "3"), from: key).wait()
        XCTAssertEqual(elements.count, 2)
        elements = try connection.zrangebyscore(within: ("1", "3"), from: key, withScores: true).wait()
        XCTAssertEqual(elements.count, 6)

        let values = try RedisConnection._mapSortedSetResponse(elements, scoreIsFirst: false)
            .map { (_, score) in return score }

        XCTAssertEqual(values[0], 1.0)
        XCTAssertEqual(values[1], 2.0)
        XCTAssertEqual(values[2], 3.0)
    }

    func test_zrevrangebyscore() throws {
        var elements = try connection.zrevrangebyscore(within: ("(1", "3"), from: key).wait()
        XCTAssertEqual(elements.count, 2)
        elements = try connection.zrevrangebyscore(within: ("1", "3"), from: key, withScores: true).wait()
        XCTAssertEqual(elements.count, 6)

        let values = try RedisConnection._mapSortedSetResponse(elements, scoreIsFirst: false)
            .map { (_, score) in return score }

        XCTAssertEqual(values[0], 3.0)
        XCTAssertEqual(values[1], 2.0)
        XCTAssertEqual(values[2], 1.0)
    }

    func test_zrangebylex() throws {
        _ = try connection.zadd([(1, 0), (2, 0), (3, 0)], to: #function).wait()

        var elements = try connection.zrangebylex(within: ("[1", "[2"), from: #function)
            .wait()
            .map { Int($0) }
        XCTAssertEqual(elements.count, 2)
        XCTAssertEqual(elements[0], 1)
        XCTAssertEqual(elements[1], 2)

        elements = try connection.zrangebylex(within: ("[1", "(4"), from: #function, limitBy: (offset: 1, count: 1))
            .wait()
            .map { Int($0) }
        XCTAssertEqual(elements.count, 1)
        XCTAssertEqual(elements[0], 2)
    }

    func test_zrevrangebylex() throws {
        _ = try connection.zadd([(1, 0), (2, 0), (3, 0), (4, 0)], to: #function).wait()

        var elements = try connection.zrevrangebylex(within: ("(2", "[4"), from: #function)
            .wait()
            .map { Int($0) }
        XCTAssertEqual(elements.count, 2)
        XCTAssertEqual(elements[0], 4)
        XCTAssertEqual(elements[1], 3)

        elements = try connection.zrevrangebylex(within: ("[1", "(4"), from: #function, limitBy: (offset: 1, count: 2))
            .wait()
            .map { Int($0) }
        XCTAssertEqual(elements.count, 2)
        XCTAssertEqual(elements[0], 2)
    }

    func test_zrem() throws {
        var count = try connection.zrem([1], from: key).wait()
        XCTAssertEqual(count, 1)
        count = try connection.zrem([1], from: key).wait()
        XCTAssertEqual(count, 0)

        count = try connection.zrem([2, 3, 4, 5], from: key).wait()
        XCTAssertEqual(count, 4)
        count = try connection.zrem([5, 6, 7], from: key).wait()
        XCTAssertEqual(count, 2)
    }

    func test_zremrangebylex() throws {
        _ = try connection.zadd([("bar", 0), ("car", 0), ("tar", 0)], to: #function).wait()

        var count = try connection.zremrangebylex(within: ("(a", "[t"), from: #function).wait()
        XCTAssertEqual(count, 2)
        count = try connection.zremrangebylex(within: ("-", "[t"), from: #function).wait()
        XCTAssertEqual(count, 0)
        count = try connection.zremrangebylex(within: ("[t", "+"), from: #function).wait()
        XCTAssertEqual(count, 1)
    }

    func test_zremrangebyrank() throws {
        var count = try connection.zremrangebyrank(within: (0, 3), from: key).wait()
        XCTAssertEqual(count, 4)
        count = try connection.zremrangebyrank(within: (0, 10), from: key).wait()
        XCTAssertEqual(count, 6)
        count = try connection.zremrangebyrank(within: (0, 3), from: key).wait()
        XCTAssertEqual(count, 0)
    }

    func test_zremrangebyscore() throws {
        var count = try connection.zremrangebyscore(within: ("(8", "10"), from: key).wait()
        XCTAssertEqual(count, 2)
        count = try connection.zremrangebyscore(within: ("4", "(7"), from: key).wait()
        XCTAssertEqual(count, 3)
        count = try connection.zremrangebyscore(within: ("-inf", "+inf"), from: key).wait()
        XCTAssertEqual(count, 5)
    }

    static var allTests = [
        ("test_zadd", test_zadd),
        ("test_zcard", test_zcard),
        ("test_zscore", test_zscore),
        ("test_zscan", test_zscan),
        ("test_zrank", test_zrank),
        ("test_zrevrank", test_zrevrank),
        ("test_zcount", test_zcount),
        ("test_zlexcount", test_zlexcount),
        ("test_zpopmin", test_zpopmin),
        ("test_zpopmax", test_zpopmax),
        ("test_zincrby", test_zincrby),
        ("test_zunionstore", test_zunionstore),
        ("test_zinterstore", test_zinterstore),
        ("test_zrange", test_zrange),
        ("test_zrevrange", test_zrevrange),
        ("test_zrangebyscore", test_zrangebyscore),
        ("test_zrevrangebyscore", test_zrevrangebyscore),
        ("test_zrangebylex", test_zrangebylex),
        ("test_zrevrangebylex", test_zrevrangebylex),
        ("test_zrem", test_zrem),
        ("test_zremrangebylex", test_zremrangebylex),
        ("test_zremrangebyrank", test_zremrangebyrank),
        ("test_zremrangebyscore", test_zremrangebyscore),
    ]
}