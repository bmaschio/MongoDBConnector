include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"

init{
  scope (ConnectionMongoDbScope){
    install (default => valueToPrettyString@StringUtils(ConnectionMongoDbScope)();
             println@Console(s)()
             );
    with (connectValue){
        .host = "localhost";
        .dbname ="prova";
        .port = 27017
      };
    connect@MongoDB(connectValue)()
}
}


main {
scope (InsertMongoTest){
  install (default => valueToPrettyString@StringUtils(ConnectionMongoDbScope)();
           println@Console(s)());
getCurrentTimeMillis@Time()(currentTime);
for (counter = 0, counter<10 , counter++ ){
    getCurrentTimeMillis@Time()(currentTime);
    q.collection = "CustomerSales";
    with (q.document){
          .name    = "Balint";
          .surname = "Maschio";
          .code = "LALA01";
          .age = 28;
          with (.purchase){
            .ammount = 30.12;
            .date.("@type")="Date";
            .date= currentTime;
            .location.street= "Mongo road";
              .location.number= 2
          }
        };
    valueToPrettyString@StringUtils (q)(s);
    println@Console("q>>>>"+s)();
    insert@MongoDB(q)(responseq);
    valueToPrettyString@StringUtils (responseq)(s);
    println@Console("responseq>>>>"+s)();

    undef(q);
    sleep@Time(10000)()
}
}

}
