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
q.filter = "{name: '$name1'}";
q.filter.name1 = "Balint";
q.documentUpdate = "{$set:{age:'$age'}}";
q.documentUpdate.age= 70;
valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
scope (updateScope){
     install (default=>  valueToPrettyString@StringUtils (updateScope)(s);
     println@Console("updateScope>>>>"+s)());
    update@MongoDB(q)(responseq)
};
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
/*issue.range = helpers.rangeFromLineNumber editor, lineStart, colStart*/
}
