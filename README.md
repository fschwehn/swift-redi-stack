<div align="center">
<p><img src="https://gitlab.com/Mordil/swift-redi-stack/wikis/uploads/cb4c517e40bd2f5ab796f1093efbd697/RediStack_social-preview_transparent.png" width="350" alt="RediStack logo"></p>

<p>
    <a href="https://gitlab.com/Mordil/swift-redis-nio-client/pipelines"><img src="https://gitlab.com/Mordil/swift-redis-nio-client/badges/master/pipeline.svg" alt="Build Status"></a>
    <a href="https://codecov.io/gl/Mordil/swift-redi-stack"><img src="https://codecov.io/gl/Mordil/swift-redi-stack/branch/master/graph/badge.svg" /></a>
    <a href="https://github.com/swift-server/sswg/blob/master/process/incubation.md#sandbox-level"><img src="https://img.shields.io/badge/sswg-sandbox-lightgrey.svg" alt="SSWG Maturity"></a>
    <a href="https://gitlab.com/Mordil/swift-redi-stack/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/License-Apache%202.0-yellow.svg" alt="Apache 2 License"></a>
    <a href="https://swift.org"><img src="https://img.shields.io/badge/Swift-5.0+-orange.svg" alt="Swift 5.0+"></a>
    <a href="https://redis.io"><img src="https://img.shields.io/badge/Redis-5.0+-red.svg" alt="Redis 5.0+"></a>
</p>
</div>

<table><thead><tr align="center"><th width="9999">
The <a href="https://github.com/Mordil/swift-redi-stack" rel="nofollow noreferrer noopener" target="_blank">GitHub repository</a> is a <b>read-only</b> mirror of the GitLab repository. For issues and merge requests, <a href="https://gitlab.com/mordil/swift-redi-stack" rel="nofollow noreferrer noopener" target="_blank">please visit GitLab</a>.
</th></tr></thead></table>

## Introduction

**RediStack** (pronounced like "ready stack") is a _non-blocking_ Swift client for [Redis](https://redis.io) built on top of [SwiftNIO](https://github.com/apple/swift-nio).

It communicates over the network using Redis' [**Re**dis **S**eralization **P**rotocol (RESP2)](https://redis.io/topics/protocol).

The table below lists the major releases alongside their compatible language, dependency, and Redis versions.

| SPM Version | [Swift](https://swift.org/download) | [Redis](https://redis.io) | [SwiftNIO](https://github.com/apple/swift-nio) | [SwiftLog](https://github.com/apple/swift-log) | [SwiftMetrics](https://github.com/apple/swift-metrics) |
|:---:|:---:|:---:|:---:|:---:|:---:|
| `from: "1.0.0-alpha.5"` | 5.0+ | 5.0+ | 2.x | 1.x | 1.x |

### Supported Operating Systems

**RediStack** runs anywhere that is officially supported by the [Swift project](https://swift.org/download/#releases).

See the [test matrix below for more details](#language-and-platform-test-matrix).

## Installing

To install **RediStack**, just add the package as a dependency in your **Package.swift**.

```swift
dependencies: [
    .package(url: "https://gitlab.com/mordil/swift-redi-stack.git", from: "1.0.0-alpha.5")
]
```

## Getting Started

**RediStack** is quick to use - all you need is an [`EventLoop`](https://apple.github.io/swift-nio/docs/current/NIO/Protocols/EventLoop.html) from **SwiftNIO**.

```swift
import NIO
import RediStack

let eventLoop: EventLoop = ...
let connection = RedisConnection.connect(
    to: try .init(ipAddress: "127.0.0.1", port: RedisConnection.defaultPort),
    on: eventLoop
).wait()

let result = try connection.set("my_key", to: "some value")
    .flatMap { return connection.get("my_key") }
    .wait()

print(result) // Optional("some value")
```

> _**Note**: Use of `wait()` was used here for simplicity. Never call this method on an `eventLoop`!_

## Documentation

The docs for the latest tagged release are always available at [docs.redistack.info](http://docs.redistack.info).

## Questions

For bugs or feature requests, file a new [issue](https://gitlab.com/mordil/swift-redi-stack/issues).

For all other support requests, please email [support@redistack.info](mailto:support@redistack.info).

## Changelog

[SemVer](https://semver.org/) changes are documented for each release on the [releases page](https://gitlab.com/Mordil/swift-redi-stack/-/releases).

## Contributing

Check out [CONTRIBUTING.md](https://gitlab.com/Mordil/swift-redi-stack/blob/master/CONTRIBUTING.md) for more information on how to help with **RediStack**.

## Contributors

Check out [CONTRIBUTORS.txt](https://gitlab.com/Mordil/swift-redi-stack/blob/master/CONTRIBUTORS.txt) to see the full list. This list is updated for each release.

## Swift on Server Ecosystem

**RediStack** is part of the [Swift on Server Working Group](https://github.com/swift-server/sswg) ecosystem - currently recommended as [**Sandbox Maturity**](https://github.com/swift-server/sswg/blob/master/process/incubation.md#sandbox-level).

| Proposal | Pitch | Discussion | Review | Vote |
|:---:|:---:|:---:|:---:|:---:|
| [SSWG-0004](https://github.com/swift-server/sswg/blob/master/proposals/0004-nio-redis.md) | [2019-01-07](https://forums.swift.org/t/swiftnio-redis-client/19325) | [2019-04-01](https://forums.swift.org/t/discussion-nioredis-nio-based-redis-driver/22455) | [2019-06-09](https://forums.swift.org/t/feedback-redisnio-a-nio-based-redis-driver/25521) | [2019-06-27](https://forums.swift.org/t/june-27th-2019/26580) |

## Language and Platform Test Matrix

The following table shows the combination of Swift language versions and operating systems that
receive regular unit testing (either in development, or with CI).

| Swift Version | macOS Mojave | Ubuntu 16.04 (Xenial) | Ubuntu 18.04 (Bionic) |
|---|:---:|:---:|:---:|
| 5.0 | X | X | X |
| 5.1 | X | X | X |
| Trunk | X | X | |

## License

[Apache 2.0](https://gitlab.com/Mordil/swift-redi-stack/blob/master/LICENSE.txt)

Copyright (c) 2019-present, Nathan Harris (@mordil)

_This project contains code written by others not affliated with this project. All copyright claims are reserved by them. For a full list, with their claimed rights, see [NOTICE.txt](https://gitlab.com/Mordil/swift-redi-stack/blob/master/NOTICE.txt)_

_**Redis** is a registered trademark of **Redis Labs**. Any use of their trademark is under the established [trademark guidelines](https://redis.io/topics/trademark) and does not imply any affiliation with or endorsement by them, and all rights are reserved by them._

_**Swift** is a registered trademark of **Apple, Inc**. Any use of their trademark does not imply any affiliation with or endorsement by them, and all rights are reserved by them._
