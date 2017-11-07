const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');
var config = require('./config');

const authProviderLocalCassandra =
new cassandra.auth.PlainTextAuthProvider(config.username, config.password);
const client = new cassandra.Client({contactPoints: [config.contactPoint], authProvider: authProviderLocalCassandra});

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
    console.log("created keyspace");    
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS cycling.race_times (race_name text, cyclist_name text, race_time text, PRIMARY KEY (race_name, race_time));";
    client.execute(query, next);
    console.log("created table");
  },
  function getMetadata(next) {
    console.log(Object.keys(client.metadata.keyspaces));
    client.metadata.getTable('cycling', 'race_times', function(err, table){
      if(!err)
      {
        console.log('Table %s', table.name);
        table.columns.forEach(function (column) {
           console.log('Column %s with type %j', column.name, column.type);
        });   
      }
      next();
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});
