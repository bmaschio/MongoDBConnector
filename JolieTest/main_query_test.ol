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
connectValue.logStreamDebug = true;
connect@MongoDB(connectValue)();
q.collection = "CustomerSales";
/*q.query = "{\"spesa.ammount\":{$gt:\"$ammount\"}}";*/
/*q.filter = "{purchase:{
  $nearSphere:{
     $geometry: '$point',
     $minDistance: '$minDist',
     $maxDistance: '$maxDist'
  }
}
}";*/
/*q.filter.point.coordinates.lat = 90.0 ;
q.filter.point.coordinates.log = 90.0 ;
q.filter.point.("@type")="Point";
q.filter.minDist = 0;
q.filter.maxDist = 10;*/

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
