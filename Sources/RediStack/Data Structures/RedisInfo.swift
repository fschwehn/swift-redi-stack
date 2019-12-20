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


public struct RedisStreamInfo {
    let length: Int
    let radixTreeKeys: Int
    let radixTreeNodes: Int
    let groups: Int
    let lastGeneratedId: String
    let firstEntry: RedisStreamEntry
    let lastEntry: RedisStreamEntry
}

extension RedisStreamInfo: Equatable {}

public struct RedisGroupInfo {
    let name: String
    let consumers: Int
    let pending: Int
    let lastDeliveredId: String
}

extension RedisGroupInfo: Equatable {}

public struct RedisConsumerInfo {
    let name: String
    let pending: Int
    let idle: Int
}

extension RedisConsumerInfo: Equatable {}
