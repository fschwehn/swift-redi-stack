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


public struct RedisXPendingSimpleResponse {
    public let pending: Int
    public let smallestPendingId: String
    public let greatestPendingId: String
    public let consumers: [Consumer]
    
    public struct Consumer {
        public let name: String
        public let pending: Int
    }
}

extension RedisXPendingSimpleResponse: Equatable {}
extension RedisXPendingSimpleResponse.Consumer: Equatable {}
