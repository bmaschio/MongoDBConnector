
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"
include "file.iol"
include "console.iol"

init{

    connectValue.host = "localhost";
    connectValue.dbname ="testData";
    connectValue.port = 27017;
    connectValue.jsonStringDebug = true;
    connectValue.timeZone = "Europe/Berlin";
    connectValue.username = "prova";
    connectValue.password = "prova";
    connectValue.logStreamDebug = true;
    connect@MongoDB(connectValue)()

}


main {
 q.collection = "CustomerSales";
 q.document.name = "ELisa";
 q.document.surname = "Draghetti";
 q.document.ammount = 10.00;
 insert@MongoDB(q)()
}
