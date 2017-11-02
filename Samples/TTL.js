const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');
const sleep = require('system-sleep');
var config = require('./config');

const authProviderLocalCassandra =
 new cassandra.auth.PlainTextAuthProvider(config.username, config.password);
const client = new cassandra.Client({contactPoints: [config.contactPoint], authProvider: authProviderLocalCassandra});

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS Cycling WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
    console.log("created keyspace");    
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS Cycling.calendar (race_id int PRIMARY KEY, race_name text, race_start_date text, race_end_date text) WITH default_time_to_live = 120";
    client.execute(query, next);
    console.log("created table");
  },
  function insert(next) {
    const queries = [
        {
            query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
            params: [1, 'Tour de France - Stage 12', '2017-10-30', '2017-10-31']
        },
        {
          query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
          params: [2, 'Tour de France - Stage 12', '2017-09-26', '2017-09-27']
        },
        {
          query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
          params: [3, 'Tour de France - Stage 13', '2017-08-01', '2017-08-02']
        },
        {
          query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
          params: [4, 'Tour de France - Stage 14', '2017-10-30', '2017-10-31']
        },
        {
          query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
          params: [5, '4th Tour of Beijing', '2017-09-15', '2017-09-16']
        },
        {
          query: 'INSERT INTO Cycling.calendar (race_id, race_name, race_start_date, race_end_date) VALUES (?,?,?,?)',
          params: [6, '4th Tour of Beijing', '2017-10-30', '2017-10-31']
        }
    ];
    client.batch(queries, { prepare: true}, next);
  },
  function selectAll(next) {
    console.log("\Select ALL");
    var query = 'SELECT * FROM Cycling.calendar';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s',row.race_id, row.race_name, row.race_start_date, row.race_end_date);
      }, this);
      next();
    });
  },
  function Sleep(next) {
      console.log("Sleeping for 2 minutes");
      sleep(125000);
      next();
  },
  function selectAll(next) {
    console.log("\Select ALL After Sleep. The table would be deleted");
    var query = 'SELECT * FROM Cycling.calendar';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s',row.race_id, row.race_name, row.race_start_date, row.race_end_date);
      }, this);
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