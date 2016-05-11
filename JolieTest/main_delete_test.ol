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
q.query = "{nome:\"$nome\"}";
q.query.nome = "Carlo";

valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
delete@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
