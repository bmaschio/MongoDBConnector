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
           connectValue.host = "localhost";
           connectValue.dbname ="prova";
           connectValue.port = 27017;
           connectValue.jsonStringDebug = true;
           connectValue.timeZone = "Europe/Berlin";
           connectValue.logStreamDebug = true;
           connect@MongoDB(connectValue)();
          listCollection@MongoDB ()(response);
          valueToPrettyString@StringUtils(response)(s);
          println@Console(s)();
         getDBReadConcern@MongoDB()(response);
         valueToPrettyString@StringUtils(response)(s);
                  println@Console(s)()
}

}
