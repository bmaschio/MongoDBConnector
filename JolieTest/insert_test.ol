
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"
include "file.iol"
include "console.iol"

init{

    connectValue.host = "localhost";
    connectValue.dbname ="test";
    connectValue.port = 27017;
    connectValue.jsonStringDebug = true;
    connectValue.timeZone = "Europe/Berlin";
    connectValue.username = "prova";
    connectValue.password = "prova";
    connectValue.logStreamDebug = true;
    connect@MongoDB(connectValue)()

}


main {

 getCurrentTimeMillis@Time () (currentTimeInMillis);
 q.collection = "mycollection";
 q.document.name = "ELisa";
 q.document.surname = "Smith";
 q.document.ammount = 10.00;
 q.document.dateInsert = currentTimeInMillis;
 q.document.dateInsert.("@type") = "Date";
 q.document.address.city= "London";
 insert@MongoDB(q)();
 q.collection = "mycollection";
 q.document.name = "John";
 q.document.surname = "Green";
 q.document.ammount = 11.00;
 q.document.dateInsert = currentTimeInMillis + 3600000L;
 q.document.dateInsert.("@type") = "Date";
 q.document.address.city= "York";
 insert@MongoDB(q)();
 q.collection = "mycollection";
 q.document.name = "Arthur";
 q.document.surname = "Green";
 q.document.ammount = 14.00;
 q.document.dateInsert = currentTimeInMillis + 7200000L;
 q.document.dateInsert.("@type") = "Date";
 q.document.address.city= "York";
 insert@MongoDB(q)()
}
