const cassandra = require('cassandra-driver');
const async = require('async');
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
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS Electricity WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
    console.log("created keyspace");    
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS Electricity.Consumption (city text, month text, year int, usage int, PRIMARY KEY(city, month, year))";
    client.execute(query, next);
    console.log("created table");
  },
  function insert(next) {

    var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var queries = [];
    for(i = 0; i < 50; i++)
    {
        var dateObj = new Date().getMonth();
        var query = {
            query: 'INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)',
            params: ['London', months[(dateObj+i)%12], 500+(10*(i+1)), 2016-(i/12)]
        }
        queries.push(query);
    }
    client.batch(queries, { prepare: true}, next);
},
function insert(next) {
    
    var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var queries = [];      
    for(i = 0; i < 50; i++)
    {
        var dateObj = new Date().getMonth();
        var query = {
            query: 'INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)',
            params: ['New York', months[(dateObj+i)%12], 2000+(10*(i+1)), 2016-(i/12)]
        }
        queries.push(query);
    }
    client.batch(queries, { prepare: true}, next);
},
function insert(next) {
    
    var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var queries = [];
      
    for(i = 0; i < 50; i++)
    {
        var dateObj = new Date().getMonth();
        var query = {
            query: 'INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)',
            params: ['Amsterdam', months[(dateObj+i)%12], 3000+(10*(i+1)), 2016-(i/12)]
        }
        queries.push(query);
    }
    client.batch(queries, { prepare: true}, next);
},
function insert(next) {
    
    var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var queries = [];
      
    for(i = 0; i < 50; i++)
    {
        var dateObj = new Date().getMonth();
        var query = {
            query: 'INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)',
            params: ['Tokyo', months[(dateObj+i)%12], 4000+(10*(i+1)), 2016-(i/12)]
        }
        queries.push(query);
    }
    client.batch(queries, { prepare: true}, next);
},
function insert(next) {
    
    var months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    var queries = [];
      
    for(i = 0; i < 50; i++)
    {
        var dateObj = new Date().getMonth();
        var query = {
            query: 'INSERT INTO Electricity.Consumption (city, month, usage, year) VALUES (?,?,?,?)',
            params: ['Paris', months[(dateObj+i)%12], 3000+(10*(i+1)), 2016-(i/12)]
        }
        queries.push(query);
    }
    client.batch(queries, { prepare: true}, next);
  },
   function selectAll(next) {
    var i=1;
    console.log("\n\nSelect ALL");
    var query = 'SELECT * FROM Electricity.Consumption';
    console.log("CityName | Month | Usage");
    const options = { prepare : true , fetchSize : 25 };
    client.eachRow(query, [], options, function (n, row) {
         console.log("Row id: %d %s | %s | %d | %d", n, row.city, row.month, row.year, row.usage);
      }, function (err, result) {
         console.log("Page Retrieved %s", result.pageState);
         if (result.nextPage) {
           console.log("Retrieving next page. Page Number = %d", i++);
           result.nextPage();
         }
         else
            return next();
      });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});
