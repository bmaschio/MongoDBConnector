include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"


  init{

      connectValue.host = "localhost";
      connectValue.dbname ="testData";
      connectValue.port = 27017;
      connectValue.jsonStringDebug = true;
      connectValue.timeZone = "Europe/Berlin";
      connectValue.username = "prova";
      connectValue.password = "prova";
      connectValue.logStreamDebug = true;
      connect@MongoDB(connectValue)()

  }



main {

q.collection = "CustomerSales";
q.filter = "{$group:{ _id : '$name', total:{$sum : '$ammount'}}}";


valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
scope (aggregateScope){
     install (default=>  valueToPrettyString@StringUtils (aggregateScope)(s);
     println@Console("updateScope>>>>"+s)());
     aggregate@MongoDB(q)(responseq)
};
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
