include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"

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
scope (InsertMongoTest){
  install (default => valueToPrettyString@StringUtils(InsertMongoTest)(s);
           println@Console(s)());
getCurrentTimeMillis@Time()(currentTime);
for (counter = 0, counter<1 , counter++ ){
    getCurrentTimeMillis@Time()(currentTime);
    q.collection = "CustomerSales";
    with (q.document){
          .("_id")= "507faa83799c";
          .("_id").("@type")="ObjectID";
          .reference.docref[0] = "507faa837999";
          .reference.docref[0].("@type")="ObjectID";
          .reference.docref[1] = "507faa837998";
          .reference.docref[1].("@type")="ObjectID";
          .name    = "Balint";
          .surname = "Maschio";
          .code = "LALA01";
          .age = 28;
          with (.poligon){
             .("@type")="Polygon";
             .coordinates[0].log= 65.4;
             .coordinates[0].lat= 65.4;
             .coordinates[1].log= 67.4;
             .coordinates[1].lat= 65.4;
             .coordinates[2].log= 65.4;
             .coordinates[2].lat= 66.4
          };
          .date = L123124321432;
          with (.date){
            .("@type")="Date"
          }
        };

    insert@MongoDB(q)(responseq);


    undef(q)

}
}

}
