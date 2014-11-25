/**
Redis Sentinel client, add-on for node_redis client.
See readme for details/usage.
*/

var RedisSingleClient = require('redis'),
    events = require('events'),
    util = require('util'),
    reply_to_object = require('redis/lib/util.js').reply_to_object,
    to_array = require('redis/lib/to_array.js'),
    commands = require('redis/lib/commands'),
    debug = require('debug')('redis-sentinel-client');
    client_call_commands = { select: 1 };

/*
options includes:
- host
- port
- masterOptions
- masterName
*/
function RedisSentinelClient(options) {

  // RedisClient takes (stream,options). we don't want stream, make sure only one.
  if (arguments.length > 1) {
    throw new Error("Sentinel client takes only options to initialize");
  }

  // make this an EventEmitter (also below)
  events.EventEmitter.call(this);

  var self = this;

  this.options = options = options || {};

  this.options.masterName = this.options.masterName || 'mymaster';

  // no socket support for now (b/c need multiple connections).
  if ((!options.port || !options.host) && (!options.sentinels || !options.sentinels.length)) {
    throw new Error("Sentinel client needs a host and port");
  }

  options.sentinels = options.sentinels || [[options.host, options.port]]

  // if debugging is enabled for sentinel client, enable master client's too.
  // (standard client just uses console.log, not the 'logger' passed here.)
  if (options.master_debug) {
    RedisSingleClient.debug_mode = true;
  }

  var masterOptions = self.masterOptions = options.masterOptions || {};
  masterOptions.disable_flush = true; // Disables flush_and_error, to preserve queue

  // if master & slaves need a password to authenticate,
  // pass it in as 'master_auth_pass'.
  // (corresponds w/ 'auth_pass' for normal client,
  // but differentiating b/c we're not authenticating to the *sentinel*, rather to the master/slaves.)
  // by setting it to 'auth_pass' on master client, it should authenticate to the master (& slaves on failover).
  // note, sentinel daemon's conf needs to know this same password too/separately.
  masterOptions.auth_pass = options.master_auth_pass || masterOptions.auth_pass;

  this.reconnectSentinel()
  this.on('sentinel disconnected', this.reconnectSentinel.bind(this))

  this.on('failover start', this.disconnect.bind(this))
  this.on('switch master', this.reconnect.bind(this))
}

util.inherits(RedisSentinelClient, events.EventEmitter);

RedisSentinelClient.prototype.reconnectSentinel = function () {
  // Upon reconnect, we end the previous connection
  if (this.sentinelTalker) this.sentinelTalker.end()
  if (this.sentinelListener) this.sentinelListener.end()

  // We get the next sentinel by rotating the array
  var sentinel = this.options.sentinels.shift()
  this.options.sentinels.push(sentinel)

  // Emit a try-connect attempt
  this.emit('sentinel connect', sentinel)
  this._connectSentinel(sentinel[1], sentinel[0])
}

RedisSentinelClient.prototype._connectSentinel = function (port, host) {
  /*
  what a failover looks like:
  - master fires ECONNREFUSED errors a few times
  - sentinel listener gets:
    +sdown
    +odown
    +try-failover
    +failover-state-wait-start
    +failover-state-select-slave
    +selected-slave
    +failover-state-send-slaveof-noone
    +failover-state-wait-promotion
    +promoted-slave
    +failover-state-reconf-slaves
    +slave-reconf-sent
    +slave-reconf-inprog
    +slave-reconf-done
    +failover-end
    +switch-master

  (see docs @ http://redis.io/topics/sentinel)

  note, these messages don't specify WHICH master is down.
  so if a sentinel is listening to multiple masters, and we have a RedisSentinelClient
  for each sentinel:master relationship, every client will be notified of every master's failovers.
  But that's fine, b/c reconnect() checks if it actually changed, and does nothing if not.
  */
  var self = this
    , isDisconnected = false

  // used for logging & errors
  this.myName = 'sentinel-' + host + ':' + port + '-' + this.options.masterName;

  // one client to query ('talker'), one client to subscribe ('listener').
  // these are standard redis clients.
  // talker is used by reconnect() below
  this.sentinelTalker = new RedisSingleClient.createClient(port, host);
  this.sentinelTalker.on('connect', function(){
    debug('connected to sentinel talker at ' + host + ':' + port);
    self.emit('sentinel connected', [host,port])

    // Start a reconnection if we are not ready already (possible to lose a sentinel connection while redis is up still)
    if (!self.ready)
      self.reconnect();
  });
  this.sentinelTalker.on('error', function(error){
    error.message = self.myName + " talker error: " + error.message + ' at ' + host + ':' + port;
    self.emit('error', error);
  });
  this.sentinelTalker.on('end', function(){
    debug('sentinel talker disconnected at ' + host + ':' + port);
    if (!isDisconnected) {
      isDisconnected = true
      self.emit('sentinel disconnected')
    }
  });

  var sentinelListener = new RedisSingleClient.createClient(port, host);
  this.sentinelListener = sentinelListener;
  sentinelListener.on('connect', function(){
    debug('connected to sentinel listener at ' + host + ':' + port);
  });
  sentinelListener.on('error', function(error){
    error.message = self.myName + " listener error: " + error.message + ' at ' + host + ':' + port;
    self.emit('error', error);
  });
  sentinelListener.on('end', function(){
    debug('sentinel listener disconnected at ' + host + ':' + port);
    if (!isDisconnected) {
      isDisconnected = true
      self.emit('sentinel disconnected')
    }
  });

  // Subscribe to all messages
  sentinelListener.psubscribe('*');

  sentinelListener.on('pmessage', function(channel, msg, args) {
    debug('sentinel message', channel, msg, args);

    // pass up, in case app wants to respond
    self.emit('sentinel message', msg);

    switch(msg) {
      case '+try-failover':
        if (args.split(' ')[1] === self.options.masterName) {
          debug('Failover detected for', self.options.masterName);
          self.emit('failover start');
          self.failovering = true;
        }
        break;

      case '+switch-master':
        if (args.split(' ')[0] === self.options.masterName) {
          debug('Switch master detected for', self.options.masterName)
          if (self.failovering) {
            self.emit('failover end');
            self.failovering = false
          }
          self.emit('switch master')
        }
        break;
    }
  });

}

// In the event of a failover, disconnecting is the prudent thing to do
// It allows an offline_queue to be built up and doesn't let the app think
// that everything is hunky dory when it isnt
RedisSentinelClient.prototype.disconnect = function disconnect() {
  this.activeMasterClient.end()
}

// [re]connect activeMasterClient to the master.
// destroys the previous connection and replaces w/ a new one,
// (transparently to the user)
// but only if host+port have changed.
RedisSentinelClient.prototype.reconnect = function reconnect() {
  var self = this;
  debug('reconnecting');

  self.sentinelTalker.send_command("SENTINEL", ["get-master-addr-by-name", self.options.masterName], function(error, newMaster) {
    if (error) {
      error.message = self.myName + " Error getting master: " + error.message;
      self.emit('error', error);
      return;
    }
    try {
      newMaster = {
        host: newMaster[0],
        port: newMaster[1]
      };
      debug('new master info', newMaster);
      if (!newMaster.host || !newMaster.port) throw new Error("Missing host or port");
    }
    catch(error) {
      error.message = self.myName + ' Unable to reconnect master: ' + error.message;
      self.emit('error', error);
    }

    if (self.activeMasterClient &&
        newMaster.host === self.activeMasterClient.host &&
        newMaster.port === self.activeMasterClient.port) {
      debug('Master has not changed, nothing to do');
      return;
    }


    debug("Changing master from " +
      (self.activeMasterClient ? self.activeMasterClient.host + ":" + self.activeMasterClient.port : "[none]") +
      " to " + newMaster.host + ":" + newMaster.port);

    self._connect(newMaster.port, newMaster.host);
  });
};

RedisSentinelClient.prototype._connect = function (port, host) {
  var self = this

    // this client will always be connected to the active master.
    , thisClient = new RedisSingleClient.createClient(port, host, self.masterOptions);

  // This hack will make it seem like the redis client has to reset its subscriptions from an old state
  // so if we were reconnecting a client in pub_sub_mode, redis will do the hard work for us!
  if (self.activeMasterClient) {
    if (self.activeMasterClient.old_state) {
      thisClient.old_state = self.activeMasterClient.old_state
    } else {
      thisClient.old_state = {
        pub_sub_mode: self.activeMasterClient.pub_sub_mode || false,
        monitoring: self.activeMasterClient.monitoring || false,
        selected_db: self.activeMasterClient.selected_db || null,
      }
    }
    thisClient.subscription_set = self.activeMasterClient.subscription_set || {}
    thisClient.offline_queue = self.activeMasterClient.offline_queue
  }

  self.activeMasterClient = thisClient;

  // pass up messages
  ;['message', 'pmessage', 'unsubscribe', 'end', 'reconnecting', 'connect', 'ready', 'error', 'subscribe'].forEach(function (evt) {
    self.activeMasterClient.on(evt, function () {
      if (self.activeMasterClient == thisClient)
        self.emit.apply(self, [evt].concat(Array.prototype.slice.call(arguments)))
    })
  })

  // @todo use no_ready_check = true? then change this 'ready' to 'connect'

  self.once(self.masterOptions.no_ready_check ? 'connect' : 'ready', function(){
    debug('New master is ready (pub_sub_mode: ' + self.pub_sub_mode + ')');
    // anything outside holding a ref to activeMasterClient needs to listen to this,
    // and refresh its reference. pass the new master so it's easier.
    self.emit('reconnected', self.activeMasterClient);
  });

};

RedisSentinelClient.prototype.queueCommand = function(args) {
  // sentinel-client has yet to establish a connect to the master
  if(!this.queuedCommands) {
    this.queuedCommands = [args];

    this.once('reconnected', function() {
      var queuedCommands = this.queuedCommands;
      delete this.queuedCommands;

      if(queuedCommands) {
        // Once reconnected occurs, activeMasterClient will exist
        var client = this.activeMasterClient;

        queuedCommands.forEach(function(q) {
          return client.send_command.apply(client, q);
        });
      }
    }.bind(this));
  } else {
    this.queuedCommands.push(args);
  }
};

//
// pass thru all client commands from RedisSentinelClient to activeMasterClient
//
RedisSentinelClient.prototype.send_command = function (command, args, callback) {
  // this ref needs to be totally atomic
  var client = this.activeMasterClient;
  if(client) {
    return client.send_command.apply(client, arguments);
  }

  var argsArray = Array.prototype.slice.apply(arguments);
  this.queueCommand(arguments);
};

// adapted from index.js for RedisClient
commands.forEach(function (command) {
  function send_command(args, callback) {
    var sentinel = this;

    debug('command', command, args);

    if (Array.isArray(args) && typeof callback === "function") {
      return sentinel.send_command(command, args, callback);
    } else {
      return sentinel.send_command(command, to_array(arguments));
    }
  }

  function call_command() {
    var sentinel = this;

    debug('command', command, arguments);

    // this ref needs to be totally atomic
    var client = this.activeMasterClient;
    return client[command].apply(client, arguments);
  }

  // Commands which require special processing in the redis-client and should therefore
  // call the redisClient command directly
  var call = client_call_commands[command];

  RedisSentinelClient.prototype[command.toUpperCase()] =
  RedisSentinelClient.prototype[command] = call ? call_command : send_command;
});

// Provide the SENTINEL command to our currently connected sentinel
RedisSentinelClient.prototype.sentinel =
RedisSentinelClient.prototype.SENTINEL =
  function (args, callback) {
    debug('sentinel command:', args)

    if (Array.isArray(args) && typeof callback === "function") {
      return this.sentinelTalker.send_command('sentinel', args, callback);
    } else {
      return sentinel.sentinelTalker.send_command('sentinel', to_array(arguments));
    }
  }

// automagically handle multi & exec?
// (tests will tell...)

// this multi is on the master client, so don't hold onto it too long!
var Multi = RedisSingleClient.Multi;

// @todo make a SentinelMulti that queues within the sentinel client?
//  would need to handle all Multi.prototype methods, etc.
//  for now let multi's queue die if the master dies.

RedisSentinelClient.prototype.multi =
RedisSentinelClient.prototype.MULTI = function (args) {
  return new Multi(this.activeMasterClient, args);
};

['hmget', 'hmset', 'done'].forEach(function(staticProp){
  RedisSentinelClient.prototype[staticProp] =
  RedisSentinelClient.prototype[staticProp.toUpperCase()] = function(){
    var client = this.activeMasterClient;
    return client[staticProp].apply(client, arguments);
  };
});

// helper to get client.
// (even tho activeMasterClient is public, this is clearer)
RedisSentinelClient.prototype.getMaster = function getMaster() {
  return this.activeMasterClient;
};

RedisSentinelClient.prototype.getSentinel = function () {
  return this.sentinelTalker
};

// commands that must be passed through to the sentinel Redises as well as the active master
;[ 'quit', 'end', 'unref' ].forEach(function(staticProp) {
  RedisSentinelClient.prototype[staticProp] =
  RedisSentinelClient.prototype[staticProp.toUpperCase()] = function(){
    this.sentinelTalker[staticProp].apply(this.sentinelTalker, arguments);
    this.sentinelListener[staticProp].apply(this.sentinelListener, arguments);
    return this.activeMasterClient[staticProp].apply(this.activeMasterClient, arguments);
  };
});


// get static values from client, also pass-thru
// not all of them... @review!
;[ 'connection_id', 'ready', 'connected', 'connections', 'commands_sent', 'connect_timeout',
  'monitoring', 'closing', 'server_info', 'pub_sub_mode', 'subscription_set',
  'stream' /* ?? */
  ].forEach(function(staticProp){
    RedisSentinelClient.prototype.__defineGetter__(staticProp, function(){
      if (this.activeMasterClient) {
        return this.activeMasterClient[staticProp];
      } else {
        return null;
      }
    });

    // might as well have a setter too...?
    RedisSentinelClient.prototype.__defineSetter__(staticProp, function(newVal){
      return this.activeMasterClient[staticProp] = newVal;
    });
  });


exports.RedisSentinelClient = RedisSentinelClient;



// called by RedisClient::createClient() when options.sentinel===true
// similar args for backwards compat,
// but does not support sockets (see above)
exports.createClient = function (port, host, options) {
  // allow the arg structure of RedisClient, but collapse into options for constructor.
  //
  // note, no default_host or default_port,
  // see http://redis.io/topics/sentinel.
  // also no net_client or allowNoSocket, see above.
  // this could be a problem w/ backwards compatibility.
  if (arguments.length === 1) {
    options = arguments[0] || {};
  }
  else {
    options = options || {};
    options.port = port;
    options.host = host;
  }

  return new RedisSentinelClient(options);
};
