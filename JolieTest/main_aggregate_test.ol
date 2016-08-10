include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"

init{
    scope (ConnectionMongoDbScope){
      install (default => valueToPrettyString@StringUtils(ConnectionMongoDbScope)();
               println@Console(s)()
               );
      with (connectValue){
          .host = "localhost";
          .dbname ="prova";
          .port = 27017;

          .jsonStringDebug = true
        };
      connect@MongoDB(connectValue)()
  }

}

main {

q.collection = "CustomerSales";
q.filter = "{$group:{ _id : '$name', total:{$sum : 1}}}";


valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
scope (aggregateScope){
     install (default=>  valueToPrettyString@StringUtils (aggregateScope)(s);
     println@Console("updateScope>>>>"+s)());
     aggregate@MongoDB(q)(responseq)
};
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()sfdsf
}
