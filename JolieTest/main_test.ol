include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
main {
println@Console("hello")();	
connectValue.host = "localhost";
connectValue.dbname ="prova";
connectValue.port = 27017;
connect@MongoDB(connectValue)();
q.table = "prove";
q.query = "{nome: { $in:[\"$name1\",\"$name2\", \"$name3\"]}}";
q.query.name1 = "Tom";
q.query.name2 = "John";
q.query.name3 = "Balint";
valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
query@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
