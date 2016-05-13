include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
main {
println@Console("hello")();
connectValue.host = "localhost";
connectValue.dbname ="prova";
connectValue.port = 27017;
connect@MongoDB(connectValue)();
q.collection = "CustomerSales";
/*q.query = "{\"spesa.ammount\":{$gt:\"$ammount\"}}";*/
q.filter = "{'age':{$gt:'$age'}}";
q.filter.age = 30;

valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
query@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
