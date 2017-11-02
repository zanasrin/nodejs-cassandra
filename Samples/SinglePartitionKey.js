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
    var query = "CREATE TABLE IF NOT EXISTS cycling.cyclist_name (id int, lastname text, firstname text, PRIMARY KEY id);";
    client.execute(query, next);
    console.log("created table");
  },
  function insert(next) {
    const queries = [
        {
           query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (1, \'PRADES\', \'Benjamin\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (2, \'PHELAN\', \'Adam\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (3, \'LEBAS\', \'Thomas\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (4, \'ZAKARIN\', \'Ilnur\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (5, \'BETANCUR\', \'Carlos\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (6, \'GILBERT\', \'Phillippe\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (7, \'MARTIN\', \'Daniel\')'
        },
        {
            query: 'INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (8, \'CHAVES\', \'Johan Esteban\')'
        }
      ];
    client.batch(queries, { prepare: true, isIdempotent: true}, next);
  },
  function selectAll(next) {
    console.log("\nSelect ALL");
    var query = 'SELECT * FROM cycling.cyclist_name';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d %s %s',row.id, row.firstname, row.lastname);
      }, this);
      next();
    });
  },
  function selectUsingPartitionKey(next) {
    console.log("\n\nSelect One Partition key");
    var query = 'SELECT * FROM cycling.cyclist_name where id = 1';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      const row = result.rows[0];
      console.log("Obtained row: %d %s %s", row.id, row.firstname, row.lastname) 
      next();
    });
  },
  function AllowFiltering(next) {
    console.log("\n\nAllow filtering");
    var query = 'SELECT * FROM cycling.cyclist_name where firstname = \'Daniel\'';
    client.execute(query, { prepare: true}, function (err, result) {
        if (err) return next(err);
        const row = result.rows[0];
        console.log("Obtained row: %d %s %s", row.id, row.firstname, row.lastname) 
        next();
  });
},
function UpdateSingleRow(next) {
    console.log("\nUpdate Single Row");
    var query = 'Update cycling.cyclist_name set lastname = \'name_changed\' where id = 1';
    client.execute(query, next);
},
function SelectSingleRow(next) {
    console.log("\Select Single Row");
    var query = 'Select * from cycling.cyclist_name set where id = 1';
    client.execute(query, function (err, result) {
        if (err) return next(err);
        const row = result.rows[0];
        console.log('Obtained row: %d %s %s',row.id, row.firstname, row.lastname);
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