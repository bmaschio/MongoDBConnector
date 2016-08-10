include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
main {
println@Console("hello")();
connectValue.host = "localhost";
connectValue.dbname ="prova";
connectValue.port = 27017;
connectValue.jsonStringDebug = true;
connect@MongoDB(connectValue)();
q.collection = "CustomerSales";
/*q.query = "{\"spesa.ammount\":{$gt:\"$ammount\"}}";*/
/*q.filter = "{'purchase.date':{$lt:'$date'}}";
q.filter.date =long("1463572271651");
q.filter.date.("@type")="Date";*/

valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
query@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
