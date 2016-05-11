include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
main {
println@Console("hello")();
connectValue.host = "localhost";
connectValue.dbname ="prova";
connectValue.port = 27017;
connect@MongoDB(connectValue)();
q.collection = "prove";
/*q.query = "{\"spesa.ammount\":{$gt:\"$ammount\"}}";*/
q.query = "{\"spesa.ammount\":{$lt:\"$eta\"}}";
q.query.eta = 30.12;

valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
query@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
