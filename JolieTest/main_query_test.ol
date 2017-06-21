include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
main {
println@Console("hello")();
connectValue.host = "localhost";
connectValue.dbname ="prova";
connectValue.port = 27017;
connectValue.jsonStringDebug = true;
connectValue.timeZone = "Europe/Berlin";
connectValue.username = "prova";
connectValue.password = "prova";
connectValue.logStreamDebug = true;
connect@MongoDB(connectValue)();
q.collection = "CustomerSales";
q.filter = "{\"reference.docref\":{$in:'$docRefList'}}";
/*q.query = "{\"spesa.ammount\":{$gt:\"$ammount\"}}";*/
/*q.filter = "{purchase:{
  $nearSphere:{
     $geometry: '$point',
     $minDistance: '$minDist',
     $maxDistance: '$maxDist'
  }
}
}";*/
q.filter.docRefList[0] = "507faa837999";
q.filter.docRefList[0].("@type")="ObjectId";
q.filter.docRefList[1] ="507faa837998";
q.filter.docRefList[1].("@type")="ObjectId";


valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
scope (myScope){
   install (default=>valueToPrettyString@StringUtils (myScope)(s);
   println@Console(s)());
    query@MongoDB(q)(responseq)
};
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
