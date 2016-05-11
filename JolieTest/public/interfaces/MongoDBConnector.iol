interface MongoDBInterface {
  RequestResponse:
  connect (undefined)(undefined) throws MongoException ,
  query (undefined)(undefined)   throws MongoException JsonParseException ,
  insert(undefined)(undefined)   throws MongoException JsonParseException ,
  update(undefined)(undefined)   throws MongoException JsonParseException ,
  delete(undefined)(undefined)   throws MongoException JsonParseException
}


outputPort MongoDB {
Interfaces: MongoDBInterface
}

embedded {
Java:
	"joliex.mongodb.MongoDbConnector" in MongoDB
}
