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
    var query = "CREATE TABLE IF NOT EXISTS cycling.rank_by_year_and_name (race_year int, race_name text, cyclist_name text, rank int, PRIMARY KEY ((race_year, race_name), rank))";
    client.execute(query, next);
    console.log("created table");
  },
  function insert(next) {
    const queries = [
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, \'Tour of Japan - Stage 4 - Minami > Shinshu\', \'Benjamin PRADES\', 1)'
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, \'Tour of Japan - Stage 4 - Minami > Shinshu\', \'Adam PHELAN\', 2)'
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, \'Tour of Japan - Stage 4 - Minami > Shinshu\', \'Thomas LEBAS\', 3)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, \'Giro d\'\'Italia - Stage 11 - Forli > Imola\', \'Ilnur ZAKARIN\', 1)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, \'Giro d\'\'Italia - Stage 11 - Forli > Imola\', \'Carlos BETANCUR\', 2)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, \'4th Tour of Beijing\', \'Phillippe GILBERT\', 1)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, \'4th Tour of Beijing\', \'Daniel MARTIN\', 2)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, \'4th Tour of Beijing\', \'Johan Esteban CHAVES\', 3)',
        },
        {
            query: 'INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2013, \'Giro d\'\'Italia - Stage 11 - Forli > Imola\', \'Daniel MARTIN\', 2)',
        },
        {
            query: 'UPDATE cycling.rank_by_year_and_name set cyclist_name = \'name-changed\' where race_year = 2013 and race_name = \'Giro d\'\'Italia - Stage 11 - Forli > Imola\' and rank = 2'
        },
        {
            query: 'UPDATE cycling.rank_by_year_and_name set cyclist_name = \'name-changed1\' where race_year = 2014 and race_name = \'4th Tour of Beijing\' and rank = 2'
        }
      ];
    client.batch(queries, { prepare: true, isIdempotent: true}, next);
  },
  function selectAll(next) {
    console.log("\nSelect ALL");
    var query = 'SELECT * FROM cycling.rank_by_year_and_name';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %d',row.race_year, row.race_name, row.cyclist_name, row.rank);
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