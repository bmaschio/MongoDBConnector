include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"

init{
  scope (ConnectionMongoDbScope){
    install (defaulta => valueToPrettyString@StringUtils(ConnectionMongoDbScope)();
             println@Console(s)()
             );
    with (connectValue){
        .host = "localhost";
        .dbname ="prova";
        .port = 27017;
        .password ="prova";
        .username = "prova";
        .timeZone = "Europe/Berlin";
        .jsonStringDebug = true
      };
    connect@MongoDB(connectValue)()
}

}

main {

q.collection = "CustomerSales";
q.filter = "{name: '$name1'}";
q.filter.name1 = "Balint";
q.documentUpdate = "{$push:{'reference':'$reference'}}";
q.documentUpdate.reference.docRef[0] = "507faa837998";
q.documentUpdate.reference.docRef[0].("@type")="ObjectID";
update@MongoDB(q)(responseq)
/*issue.range = helpers.rangeFromLineNumber editor, lineStart, colStart*/
}
