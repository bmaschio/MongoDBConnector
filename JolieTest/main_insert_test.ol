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
    valueToPrettyString@StringUtils (q)(s);
    println@Console("q>>>>"+s)();
    insert@MongoDB(q)(responseq);
    valueToPrettyString@StringUtils (responseq)(s);
    println@Console("responseq>>>>"+s)();

    undef(q)

}
}

}
