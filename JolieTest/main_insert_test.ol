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
q.document.nome = "Luca";
q.document.cognome = "Rossi";
q.document.eta = 20;
q.document.spesa[0].ammount = 20.12;
q.document.spesa[1].ammount = 21.12;
q.document.spesa[2].ammount = 30.12;
valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
insert@MongoDB(q)(responseq);
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
