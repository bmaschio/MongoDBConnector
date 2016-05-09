interface MongoDBInterface {
  RequestResponse:
  connect (undefined)(undefined),
  query (undefined)(undefined),
  insert(undefined)(undefined),
  update(undefined)(undefined)
}


outputPort MongoDB {
Interfaces: MongoDBInterface
}

embedded {
Java:
	"joliex.mongodb.MongoDbConnector" in MongoDB
}
