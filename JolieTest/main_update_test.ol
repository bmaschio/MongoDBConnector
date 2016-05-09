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
q.document.eta = 35;
q.query = "{nome: \"$name1\"}";
q.query.name1 = "Carlo";
valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
update@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
