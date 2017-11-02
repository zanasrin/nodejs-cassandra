---
services: cosmos-db
platforms: nodejs
author: zanasrin
---

# Developing a Cassandra-Node.js app using Azure Cosmos DB
Azure Cosmos DB is Microsoft’s globally distributed multi-model database service. You can quickly create and query document, key/value, and graph databases, all of which benefit from the global distribution and horizontal scale capabilities at the core of Azure Cosmos DB.

This quickstart demonstrates how to write a Node.js app and connect it to your Azure Cosmos DB database, which supports Cassandra client connections. In other words, your Node.js application only knows that it's connecting to a database using Cassandra APIs. It is transparent to the application that the data is stored in Azure Cosmos DB.

## Running this sample
* Before you can run this sample, you must have the following perquisites:
	* An active Azure DocumentDB account - If you don't have an account, refer to the [Create a DocumentDB account](https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/) article.
	* [Node.js](https://nodejs.org/en/) version v0.10.29 or higher.
	* [Git](http://git-scm.com/).


1. Clone this repository using `git clone https://github.com/zanasrin/nodejs-cassandra.git`

2. Go to Samples folder.

3. Next, substitute the endpoint and authorization key in `config.js` with your Cosmos DB account's values.

	```
	config.username = "~your DocumentDB endpoint here~";
	config.password = "~your auth key here~";
	```

4. Run `npm install` in a terminal to install required npm modules
 
5. Run `node FileName.js` in a terminal to execute it.

## About the code
The code included in this sample is intended to get you quickly started with a Node.js console application that connects to Azure Cosmos DB with the Cassandra API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure DocumentDB Node.js SDK](https://docs.microsoft.com/azure/documentdb/documentdb-sdk-node)
- [Azure DocumentDB Node.js SDK Reference Documentation](http://azure.github.io/azure-documentdb-node/)
