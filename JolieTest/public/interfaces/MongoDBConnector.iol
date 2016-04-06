interface MongoDBInterface {
  RequestResponse: 
  connect (undefined)(undefined),
  query (undefined)(undefined)
}


outputPort MongoDB {
Interfaces: MongoDBInterface
}

embedded {
Java:
	"joliex.mongodb.MongoDbConnector" in MongoDB
}