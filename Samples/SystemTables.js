const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');
var config = require('./config');
var fs = require('fs'); 

var certificate = fs.readFileSync(config.cert, 'utf8');
var options = {
    cert: certificate,
    secureProtocol: 'TLSv1_2_method'
  };

const authProviderLocalCassandra =
new cassandra.auth.PlainTextAuthProvider(config.username, config.password);
const client = new cassandra.Client({contactPoints: [config.contactPoint], authProvider: authProviderLocalCassandra, sslOptions: options});
  
async.series([
  function connect(next) {
    client.connect(next);
  },
  function SchemaKeyspaces(next) {
    console.log("\n\nSchema keyspaces by query");
    var query = 'SELECT * FROM system_schema.keyspaces';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %s',row.keyspace_name);
      }, this);
      next();
    });
  },
  function SchemaTables(next) {
    console.log("\n\nSchema tables by query");
    var query = 'SELECT table_name FROM system_schema.tables';
    client.execute(query, { prepare: true}, function (err, result) {
      if (err) return next(err);
      result.rows.forEach(function(row) {
        console.log('Obtained row: %s',row.table_name);
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
