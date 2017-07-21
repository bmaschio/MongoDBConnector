type ConnectRequest:void{
  .host : string
  .dbname : string
  .port :int
  .timeZone:string
  .username:string
  .password:string
  .jsonStringDebug?:bool
  .logStreamDebug?:bool
}

type ConnectResponse: void

type QueryRequest:void{
   .collection: string
   .filter?:undefined
   .sort?:undefined
   .limit?: int
   .readConcern?:string
}

type QueryResponse:void{
   .document*: undefined
}
type InsertRequest:void{
  .collection: string
  .document:undefined
  .writeConcern?: void{
     .w?:any
     .journal?: bool
     .timeout:long
  }
}

type InsertResponse:void{
  ._id:string{
   ?
  }
}

type InsertManyRequest:void{
  .collection: string
  .document*:undefined
}


type InsertManyResponse:void{
   .results*:void{
     ._id:string{
      ?
     }
   }
}
type UpdateRequest:void{
  .collection: string
  .filter:undefined
  .documentUpdate:undefined
}

type UpdateResponse:void{
  .matchedCount:long
  .modifiedCount:long
}


type UpdateManyRequest:void{
  .collection: string
  .filter:undefined
  .documentUpdate:undefined
}

type UpdateManyResponse:void{
   .matchedCount:long
   .modifiedCount:long
}

type DeleteRequest:void{
  .collection: string
  .filter?:undefined
}

type DeleteResponse:void{
  .deletedCount:long
}

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
  insertMany ( InsertManyRequest) (InsertManyResponse) throws MongoException JsonParseException,
  update  (UpdateRequest)(UpdateResponse)   throws MongoException JsonParseException ,
  updateMany  (UpdateManyRequest)(UpdateManyResponse)   throws MongoException JsonParseException ,
  delete  (DeleteRequest)(DeleteResponse)   throws MongoException JsonParseException ,
  deleteMany  (DeleteRequest)(DeleteResponse)   throws MongoException JsonParseException ,
  aggregate (AggregateRequest)(AggregateResponse) throws MongoException JsonParseException,
  listCollection(ListCollectionRequest)(ListCollectionResponse) throws MongoException JsonParseException
}


outputPort MongoDB {
Interfaces: MongoDBInterface
}

embedded {
Java:
	"joliex.mongodb.MongoDbConnector" in MongoDB
}
