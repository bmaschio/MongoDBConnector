include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"


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


q.collection = "CustomerSales";
with (q.document){
      .name    = "Lars";
      .surname = "Larsesen";
      .code = "LALA01";
      .age = 28;
      with (.purchase){
        .ammount = 30.12;
        .date = "12.03.2016"
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
