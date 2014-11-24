# Redis Sentinel Client for Node.js


Supplements [node_redis](https://github.com/mranney/node_redis) with [Redis Sentinel](http://redis.io/topics/sentinel) support.

From the Sentinel docs:

> Redis Sentinel is a system designed to help managing Redis instances. It performs the following three tasks:
> **Monitoring.** Sentinel constantly check if your master and slave instances are working as expected.
> **Notification.** Sentinel can notify the system administrator, or another computer program, via an API,
> that something is wrong with one of the monitored Redis instances.
> **Automatic failover.** If a master is not working as expected, Sentinel can start a failover process
> where a slave is promoted to master, the other additional slaves are reconfigured to use the new master,
> and the applications using the Redis server informed about the new address to use when connecting.


## Goals

1. Transparent, drop-in replacement for RedisClient, handling connections to master, slave(s), and sentinel in the background.
2. Handles all RedisClient operations (including pub/sub).
3. Minimize data loss

This was originally part of a [fork of node_redis](https://github.com/DocuSignDev/node_redis),
and has been subsequently split to its own module.
(However, it still currently requires changes to node_redis to work, so it still depends on the fork.)

See related thread about different approaches to Sentinel support: https://github.com/mranney/node_redis/issues/302


## Concepts

- connects to a single or multiple sentinels, which is watching a master/slave(s) cluster
- maintains a query and subscribe connection to the active sentinel connection (which rotates on failure), and a single redis client connection to the master of the cluster in the background, which automatically updates on `switch master`
- behaves exactly like a single RedisClient (all methods are passthrough)

## Usage

`npm install redis-sentinel-client`

```
var RedisSentinel = require('redis-sentinel-client');
var sentinelClient = RedisSentinel.createClient(options);
// or
var sentinelClient = RedisSentinel.createClient(PORT, HOST [, options]);
```

Now use `sentinelClient` as a regular client: `set`, `get`, `hmset`, etc.

## Instantiation options

- Sentinel Connection Options (1 required):
    - `host` and `port`: Connect to a single sentinel
    - `sentinels`: Keep a list of all sentinels in the cluster so that if one disconnects, we rotate to another (Alternative to `port` and `host`): `sentinels: [[host1,port1],[host2,port2]]`
- `masterName`: Which master the sentinel is listening to. Defaults to 'mymaster'. (If a sentinel is listening to multiple masters, create multiple `SentinelClients`.)
- `masterOptions`: The options object which will be passed on to the Redis master client connection. See the [node_redis](https://github.com/mranney/node_redis#rediscreateclientport-host-options) documentation for more details.
- `master_auth_pass`: If your master and slave(s) need authentication (`options.auth_pass` in node_redis, as of 0.8.5), this is passed on. (Note, the sentinel itself does not take authentication.)
- `master_debug`: Make the master connections be debug connections


## Methods

- `getMaster()`: returns a reference to the sentinel client's `activeMasterClient` (also available directly). The use of this method is not recommended as clients are thrown away after disconnection and a new one is instantiated.
- `getSentinel()`: returns a refrence to the sentinel client itself
- `reconnect()`: used on instantiation and on psub events, this checks if the master has changed and connects to the new master.
- `send_command()` (and any other `RedisClient` command): command is passed to the master client.
- `sentinel()` A redis command sent to the sentinel instance. (for instance `cli.sentinel('masters', 'mymaster', callback)`)


## Events

In addition to passing through all RedisClient events from the master connection, the following are emitted from the sentinel wrapper.

- `.emit('sentinel connect', [host,port])`: Emitted when sentinel connection is starting
- `.emit('sentinel connected', [host,port])`: Emitted when sentinel connection is established
- `.emit('sentinel disconnected')`: Emitted when sentinel connection is lost
- `.emit('sentinel message', msg)`: Emitted from the subscription to all sentinel messages. Note, messages can be about other masters, does not differentiate.
- `.emit('failover start')`: Emitted when a failover is beginning.
- `.emit('failover end')`: Emitted when a failover has ended.
- `.emit('switch master')`: Emitted when the master is switching (failover or not)
- `.emit('error', err)`: Emitted when an error occured. You should listen on this so that node does not trigger an uncaught exception


## Tests

There is now one large test, run with [Mocha](https://github.com/visionmedia/mocha). It starts up redis (you should have the `redis-server` executable with sentinel support built in installed) and tests the connection under a number of failure conditions.

```
npm install
npm test
```

_Note_: This module uses [debug](https://github.com/visionmedia/debug), so to see debug output, simply use: `DEBUG=redis-sentinel-client,redis-processes npm test`


## Limitations

- Unlike `RedisClient`, `SentinelClient` is not / does not need to be a stream
- Sentinel docs don't specify a default host+port, so option-less implementations of `createClient()` won't be compatible.
- Have not put any time into `multi` support, unknown status.

## Possible roadmap

- Multiple master/slave(s) clusters per sentinel
  - But thinking not: Just create multiple sentinel clients, one per cluster.


## Credits

Created by the [Node.js team at DocuSign](https://github.com/DocuSignDev) (in particular [Ben Buckman](https://github.com/newleafdigital) and [Derek Bredensteiner](https://github.com/proksoup)).

Major modifications made by [Jon Eisen](https://github.com/yanatan16) at [Rafflecopter](https://github.com/Rafflecopter).

## License

MIT
