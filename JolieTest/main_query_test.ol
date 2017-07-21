include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"

interface TestInteface{
  RequestResponse:
     read(undefined)(undefined)
}
inputPort HttpPort {
Location: "socket://localhost:9001"
Protocol: http
Interfaces: TestInteface
}

/*60379760*/

[{$match{'prov': 'Milano'}},{$group{_id:'$citta',total:{$sum : 1}}}]

execution{ concurrent }
init {
  connectValue.host = "localhost";
  connectValue.dbname ="ClientData";
  connectValue.port = 27017;
  connectValue.jsonStringDebug = false;
  connectValue.timeZone = "Europe/Berlin";
  connectValue.username = "prova";
  connectValue.password = "prova";
  connectValue.logStreamDebug = true;
  connect@MongoDB(connectValue)()
}
main {
[read (request)(response){
    q.collection = "Contact";
    q.filter = "{'id_contatto':'$id_contatto'}";
    q.filter.id_contatto = int (request.id_contatto);
    query@MongoDB(q)(response)
  }]

}
