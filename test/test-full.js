/*
test a failover scenario
should lose no data (atomic set/get or pub/sub) during the failover.

to use this,
  - ./node_modules/.bin/mocha --ui tdd --reporter spec --bail test/test-full
*/

var assert = require('assert'),
    RedisSentinel = require('../index'),
    RedisClient = require('redis'),
    start = require('./start-redis'),
    async = require('async'),
    events = require('events'),
    util = require('util'),
    debug = require('debug')('test'),
    password = 'h3rr0'

var ports = {
  redis1: 5379,
  redis2: 5380,
  sentinel1: 8379,
  sentinel2: 8380
}

describe('sentinel full', function () {
  describe('redis db 0', runTests(0));
  describe('redis db 1', runTests(1));
})

function runTests(dbNumber) {
  var _suite;

  return function() {

    // (want setup to run once, using BDD-style `before`)
    before(function (done) {
      _suite = {};

      _suite.hashKey = "test-sentinel-" + Math.round(Math.random() * 1000000);
      _suite.key = function (s) { return _suite.hashKey + ":" + s }
      debug("Using test hash", _suite.hashKey)

      // start up external redis'
      _suite.processes = {
        redis1: start.redis('./test/redis1.conf', ports.redis1, null, password),
        redis2: start.redis('./test/redis2.conf', ports.redis2, ports.redis1, password),
        sentinel1: start.sentinel('./test/sentinel1.conf', ports.sentinel1, ports.redis1, password),
        sentinel2: start.sentinel('./test/sentinel2.conf', ports.sentinel2, ports.redis1, password)
      }

      setTimeout(onTimeout, 1000)

      function onTimeout() {
        _suite.clients = {
          redis1: RedisClient.createClient(ports.redis1,null,{auth_pass:password}),
          redis2: RedisClient.createClient(ports.redis2,null,{auth_pass:password}),
          sentinel1: RedisClient.createClient(ports.sentinel1),
          sentinel2: RedisClient.createClient(ports.sentinel2)
        }
        _suite.masterClient = _suite.clients.redis1
        done()
      }

      _suite.createSentinelClient = function (dbg) {
        var client = RedisSentinel.createClient({
          sentinels: [
            ['127.0.0.1', 8379],
            ['127.0.0.1', 8380]
          ],
          masterName: 'testmaster',
          master_auth_pass: password,
          master_debug: dbg
        }).on('error', function (err) {
          if (err.name == 'AssertionError') {
            throw err
          } else {
            debug('ERROR', err)
          }
        })

        if(dbNumber) {
          client.select(dbNumber, function(err) {
            if(err) throw err
          })
        }

        return client;
      }
    }); //setup

    it('should allow commands issues before master connection is established', function (done) {
      var cli = _suite.createSentinelClient()
      var d = Date.now();

      cli.set('before.connection', d, function(err) {
        assert.ifError(err)

        cli.get('before.connection', function(err, value) {
          assert.ifError(err)
          assert.equal(value, d)
          done()
        });
      });

    })


    it('should redis master is ready', function (done) {
      var cli = _suite.clients.redis1
      cli.ready ? done() : cli.once('ready', done)
    })

    it('should redis slave is ready', function (done) {
      var cli = _suite.clients.redis2
      cli.ready ? done() : cli.once('ready', done)
    })

    it('should redis slaveof is working', function (done) {
      var cli1 = _suite.clients.redis1
        , cli2 = _suite.clients.redis2

      cli1.set(_suite.key('getset'), _suite.hashKey, function (err) {
        assert.ifError(err)
        cli1.get(_suite.key('getset'), function (err, rval) {
          assert.ifError(err)
          assert.equal(rval, _suite.hashKey)

          cli2.get(_suite.key('getset'), function (err, sval) {
            assert.ifError(err)
            assert.equal(rval, _suite.hashKey)
            done()
          })
        })
      })
    })

    it('should sentinel1 ready', function (done) {
      var cli = _suite.clients.sentinel1
      cli.ready ? done() : cli.once('ready', done)
    })

    it('should sentinel ready', function (done) {
      var cli = _suite.clients.sentinel2
      cli.ready ? done() : cli.once('ready', done)
    })

    it('should sentinel1 monitor redis', function (done) {
      var cli = _suite.clients.sentinel1
      cli.send_command('sentinel', ['master', 'testmaster'], function (err) {
        assert.ifError(err)
        done()
      })
    })

    it('should sentinel2 monitor redis', function (done) {
      var cli = _suite.clients.sentinel2
      cli.send_command('sentinel', ['master', 'testmaster'], function (err) {
        assert.ifError(err)
        done()
      })
    })

    it('should setup sentinel client', function (done) {
      var cli = _suite.clients.sentinelClient = _suite.createSentinelClient()
      cli.on('error', function (err) {
        console.log('Error in sentinel client:', err.stack || err)
      })
      cli.once('ready', function (err) {
        done(err)
        startContinuity(cli)
      })
    })

    it('should setup sentinel pubsub', function (done) {
      var pub = _suite.clients.sentinelPub = _suite.createSentinelClient()
        sub =  _suite.clients.sentinelSub = _suite.createSentinelClient()

      var done2 = donen(2, done)

      sub.once('ready', function () {
        sub.on('subscribe', function () { done2() })
        sub.subscribe(_suite.key('counter'))

        // Test the subscriber is back in subscriber mode every time ready is called
        sub.on('ready', function () {
          assert.ok(sub.pub_sub_mode)
          assert.ok(Object.keys(sub.subscription_set).length > 0)
        })
      })
      pub.once('ready', function () {
        done2()
      })
    })

    it('should use sentinel client after ready', testSentinelClient)

    it('should who is master', function (done) {
      _suite.clients.sentinel1.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis1)
        done()
      })
    })

    it('should failover', function (done) {
      this.timeout(20000)
      setTimeout(onTimeout.bind(null, 0), 2000)

      var done2 = donen(2, done)
      _suite.clients.sentinelClient.once('failover start', done2.bind(null, null))
      _suite.clients.sentinelClient.once('failover end', done2.bind(null, null))

      function onTimeout(cnt) {
        _suite.clients.sentinel1.send_command('sentinel', ['failover', 'testmaster'], function (err) {
          if (cnt<9 && err && /NOGOODSLAVE/.test(err.toString())) {
            debug('Cant failover yet('+cnt+'): ', err.toString())
            return setTimeout(onTimeout.bind(null, cnt+1), 2000)
          }
          debug('redis failover started')
          assert.ifError(err)
        })
      }
    })

    it('should who is master', function (done) {
      _suite.clients.sentinel1.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis2)
        done()
      })
    })

    it('should use sentinel client after failover', testSentinelClient)

    it('should configuration propogation', function (done) {
      this.timeout(4000)
      setTimeout(function () {
        _suite.clients.sentinel2.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
          assert.ifError(err)
          assert.equal(bulk[1], ports.redis2)
          done()
        })
      }, 2000)
    })

    it('should set sdown detection low', function (done) {
      _suite.clients.sentinel2.send_command('sentinel',['set', 'testmaster', 'down-after-milliseconds', 1000], done)
    })

    it('should kill master', function (done) {
      this.timeout(40000)
      setTimeout(onTimeout, 10000)

      var done4 = donen(4, done)
      _suite.clients.sentinelClient.once('switch master', done4).once('ready', done4)
      _suite.clients.sentinelPub.once('ready', done4)
      _suite.clients.sentinelSub.once('ready', function () {
        assert.ok(sub.pub_sub_mode)
        assert.ok(Object.keys(sub.subscription_set).length > 0)
        done4()
      })

      function onTimeout() {
        _suite.clients.redis2.end()

        _suite.processes.redis2.kill()
        delete(_suite.clients.redis2)
        delete(_suite.processes.redis2)

        debug('redis master killed')
      }
    })

    it('should return who is master', function (done) {
      _suite.clients.sentinel2.send_command('sentinel', ['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis1)
        done()
      })
    })

    it('should start slave back', function (done) {
      this.timeout(10000)
      _suite.processes.redis2 = start.redis('./test/redis2.conf', ports.redis2, ports.redis1, password)
      setTimeout(function () {
        _suite.clients.redis2 = RedisClient.createClient(ports.redis2,null,{auth_pass:password}).on('ready', done)
      }, 5000)
    })

    it('should use sentinel client after master kill', testSentinelClient)

    it('should kill other master', function (done) {
      this.timeout(30000)
      setTimeout(onTimeout, 4000)
      _suite.clients.sentinelClient.once('switch master', done)

      function onTimeout() {
        _suite.clients.redis1.end()
        _suite.processes.redis1.kill()
        delete(_suite.clients.redis1)
        delete(_suite.processes.redis1)

        debug('redis master killed')
      }
    })

    it('should return who is master', function (done) {
      _suite.clients.sentinelClient.sentinel(['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis2)
        done()
      })
    })

    it('should use sentinel client after second master kill', testSentinelClient)

    it('should kill sentinel1', function (done) {
      this.timeout(10000)
      setTimeout(onTimeout, 1)

      function onTimeout() {
        _suite.clients.sentinel1.end()
        _suite.processes.sentinel1.kill()
        delete(_suite.clients.sentinel1)
        delete(_suite.processes.sentinel1)

        var done3 = donen(3, done)
        _suite.clients.sentinelClient.once('sentinel disconnected', done3)
        _suite.clients.sentinelClient.once('sentinel connect', done3.bind(null,null))

        _suite.clients.sentinelClient.once('sentinel connected', function (pair) {
          assert.equal(pair[1], ports.sentinel2)
          done3()
        })
      }
    })

    it('should use sentinel client after sentinel kill', testSentinelClient)

    it('should use SENTINEL command through client', function (done) {
      var cli = _suite.clients.sentinelClient

      cli.sentinel(['get-master-addr-by-name', 'testmaster'], function (err, bulk) {
        assert.ifError(err)
        assert.equal(bulk[1], ports.redis2)
        done()
      })
    })

    it('should test continuity through whole test', function (done) {
      testContinuity(_suite.clients.sentinelClient, done)
    })

    after(function (done) {
      Object.keys(_suite.clients).forEach(function (cli) { _suite.clients[cli].end() })
      Object.keys(_suite.processes).forEach(function (proc) { _suite.processes[proc].kill() })
      setTimeout(done, 1000)
    })

  }


  process.on('uncaughtException', function (err) {
    Object.keys(_suite.clients).forEach(function (cli) { _suite.clients[cli].end() })
    Object.keys(_suite.processes).forEach(function (proc) { _suite.processes[proc].kill() })
  })

  function donen(n, done) {
    var cnt = 0
    return function (err) {
      cnt++
      if (err && cnt <= n) {
        cnt = n
        done(err)
      } else if (cnt === n) {
        done()
      }
    }
  }

  function testSentinelClient(done) {
    async.parallel([
      testGetSet.bind(null, _suite.clients.sentinelClient, _suite.key('getset')),
      testHmGetSet.bind(null, _suite.clients.sentinelClient, _suite.key('hmgetset')),
      testPubSub.bind(null, _suite.clients.sentinelPub, _suite.clients.sentinelSub)
    ], done)
  }

  function testGetSet(cli, key, cb) {
    var val = new Date().toString()
    cli.set(key, val, function (err) {
      assert.ifError(err)
      cli.get(key, function (err, rval) {
        assert.ifError(err)
        assert.equal(rval, val)
        cb()
      })
    })
  }

  function testHmGetSet(cli, key, cb) {
    var k1 = Date.now().toString()
      , v1 = new Date().toString()
      , obj = { 'other': 'things' }
    obj[k1] = v1

    debug('testing hmget and hmset at key', key, 'with value', obj)
    cli.hmset(key, obj, function (err) {
      assert.ifError(err)
      cli.hmget(key, k1, 'other', function (err, reply) {
        assert.ifError(err)
        debug('hmget response', reply, {k1:v1})
        assert.equal(reply[0], v1)
        assert.equal(reply[1], 'things')
        cb()
      })
    })
  }

  function testPubSub(pub, sub, cb) {
    var val = Date.now()
    sub.once('message', function (chan, sval) {
      debug('pub sub message received!')
      assert.equal(chan, _suite.key('counter'))
      assert.equal(sval, val)
      cb()
    })
    pub.publish(_suite.key('counter'), val, assert.ifError)
  }

  function startContinuity(cli) {
    var key = _suite.key('continuity')
      , i = 1
    setInterval(function () {
      var j = i++
      cli.rpush(key, j, function (err) {
        if (err) {
          debug('Error on continuity! ' + j + ': ' + err)
          i--
        } else {
          debug('Sent continuity ' + j)
      }
      })
    }, 300)
  }

  function testContinuity(cli, cb) {
    cli.lrange(_suite.key('continuity'), 0, -1, function (err, data) {
      debug('continuity received ' + JSON.stringify(data))
      assert.ifError(err)
      assert.ok(data.length > 0)
      assert.deepEqual(data, Array.apply(null, Array(data.length)).map(function (_,i) { return ''+(i+1) }))
      cb()
    })
  }

}
