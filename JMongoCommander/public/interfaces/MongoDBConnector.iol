type ConnectRequest:void{
  .host : string
  .dbname : string
  .port :int
  .timeZone:string
  .jsonStringDebug?:bool
  .logStreamDebug?:bool
}

type ConnectResponse: void

type QueryRequest:void{
   .collection: string
   .filter?:undefined
   .sort?:undefined
   .limit?: int
}

type QueryResponse:void{
   .document*: undefined
}
type InsertRequest:void{
  .collection: string
  .document:undefined
}

type InsertResponse:void


type UpdateRequest:void{
  .collection: string
  .filter:undefined
  .documentUpdate:undefined
}

type UpdateResponse:void

type DeleteRequest:void{
  .collection: string
  .filter?:undefined
}

type DeleteResponse:void

type AggregateRequest:void{
    .collection: string
    .filter*:string{
       ?
    }
}

type AggregateResponse:void{
    .document*:undefined
}

type ListCollectionRequest:undefined

type ListCollectionResponse:void{
  .collection*:string
}
interface MongoDBInterface {
  RequestResponse:
  connect (ConnectRequest)(ConnectResponse) throws MongoException ,
  query   (QueryRequest)(QueryResponse)   throws MongoException JsonParseException ,
  insert  (InsertRequest)(InsertResponse)   throws MongoException JsonParseException ,
  update  (UpdateRequest)(UpdateResponse)   throws MongoException JsonParseException ,
  delete  (DeleteRequest)(DeleteResponse)   throws MongoException JsonParseException ,
  aggregate (AggregateRequest)(AggregateResponse) throws MongoException JsonParseException,
  listCollection(ListCollectionRequest)(ListCollectionResponse) throws MongoException JsonParseException,
  getDBReadConcern(undefined)(undefined)
}


outputPort MongoDB {
Interfaces: MongoDBInterface
}

embedded {
Java:
	"joliex.mongodb.MongoDbConnector" in MongoDB
}
