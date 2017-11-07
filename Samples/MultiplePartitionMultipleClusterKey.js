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
    var query = "CREATE KEYSPACE IF NOT EXISTS Music WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
    console.log("created keyspace");    
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS Music.playlists (votes int, song_name text, artist_name text, album_name text, year text, PRIMARY KEY ((song_name, artist_name), album_name, year))";
    client.execute(query, next);
    console.log("created table");
  },
  function insert(next) {
    const queries = [
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [cassandra.types.unset, 'Despacito', 'Luis Fonsi', 'undefined', '2017']
        },
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [undefined, 'Shape of You', 'Ed Sheeran', 'undefined', '2017']
        },
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [3000, 'If I could fly', 'One Direction', 'Made in the A.M', '2015']
        },
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [4000, 'Happily', 'One Direction', 'Midnight Memories', '2013']
        },
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [5000, 'One more night', 'Maroon 5', 'Overexposed', '2012']
        },
        {
            query: 'INSERT INTO Music.playlists (votes, song_name, artist_name, album_name, year) VALUES (?,?,?,?,?)',
            params: [6000, 'Sugar', 'Maroon 5', 'V', '2014']
        }
    ];
    client.batch(queries, { prepare: true}, next);
  },
  function selectAll(next) {
    console.log("\nSelect ALL");
    var query = 'SELECT * FROM Music.playlists';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
      }, this);
      next();
    });
  },
  function selectUsingSinglePartitionKey(next) {
    console.log("\n\nSelect Using One Partition key");
    var query = 'SELECT * FROM Music.playlists where artist_name = \'Maroon 5\' ALLOW FILTERING';
    client.execute(query, { prepare: true}, function (err, result) {
        if (err) return next(err);
        result.rows.forEach(function(row) {
            console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
        }, this);
        next();
      });
  },
  function UpdateOneRow(next) {
    console.log("\n\nUpdate One Row");
    var query = 'Update Music.playlists set votes = 10000 where song_name=\'Despacito\' and artist_name=\'Luis Fonsi\' and album_name = \'undefined\' and year = \'2017\'';
    client.execute(query, { prepare: true}, 
        next);
  },
  function selectAll(next) {
    console.log("\nSelect ALL");
    var query = 'SELECT * FROM Music.playlists';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
      }, this);
      next();
    });
  },
  function selectLimit(next) {
    console.log("\nSelect LIMIT 3");
    var query = 'SELECT * FROM Music.playlists LIMIT 3';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
      }, this);
      next();
    });
  },
  function selectComparisonOperator(next) {
    console.log("\nSelect with comparison operator");
    var query = 'SELECT * FROM Music.playlists where votes > 4000 ALLOW FILTERING';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
      }, this);
      next();
    });
  },
  function DeleteRow(next) {
    console.log("\nDelete Row");
    var query = 'DELETE FROM Music.playlists where song_name=\'Despacito\' and artist_name=\'Luis Fonsi\' and album_name = \'undefined\' and year = \'2017\'';
    client.execute(query, next);
  },
  function selectAll(next) {
    console.log("\nSelect ALL");
    var query = 'SELECT * FROM Music.playlists';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %d | %s | %s | %s | %s',row.votes, row.song_name, row.artist_name, row.album_name, row.year);
      }, this);
      next();
    });
  },
  function Schema(next) {
    console.log("\n\nSchema keyspaces by query");
    var query = 'SELECT * FROM system_schema.keyspaces';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %s %s',row.keyspace_name, row.strategy_option);
      }, this);
      next();
    });
  },
  function DropTable(next)
  {
    console.log("\n\nDropping table");
    var query = 'DROP Table Music.playlists';
    client.execute(query, next);
  },
  function DropKeyspace(next)
  {
    console.log("Dropping Keyspace");
    var query = 'DROP Keyspace Music';
    client.execute(query, next);
  }  
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});
