include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "file.iol"

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

/*[{$match{'prov': 'Milano'}},{$group:{_id:'$citta',total:{$sum : 1}}}]*/

execution{ concurrent }
init {
  connectValue.host = "10.101.50.107";
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


    query@MongoDB(q)(response);
    fileContent =" ";
    for (counter = 0 , counter< #response.document  , counter++ ){
      if (response.document[counter].intestazione != ""){
        fileContent+= response.document[counter].intestazione + ";"
      }else{
        fileContent+= ";"
      };
      if (response.document[counter].nome != ""){
        fileContent+= response.document[counter].nome + ";"
      }else{
        fileContent+= ";"
      };
      if (response.document[counter].cognome != ""){
        fileContent+= response.document[counter].cognome + ";"
      }else{
        fileContent+= ";"
      };

      if (response.document[counter].societa != ""){
        fileContent+= response.document[counter].societa + ";"
      }else{
        fileContent+= ";"
      };

      if (is_defined (response.document[counter].mail)){
        fileContent+= response.document[counter].mail + ";"
      }else{
        fileContent+= ";"
      };
      if (is_defined (response.document[counter].tel)){
        fileContent+= response.document[counter].tel + ";"
      }else{
        fileContent+= ";"
      };
      if (is_defined (response.document[counter].regione)){
        fileContent+= response.document[counter].regione + ";"
      }else{
        fileContent+= ";"
      };
      if (is_defined (response.document[counter].prov)){
        fileContent+= response.document[counter].prov + ";"
      }else{
        fileContent+= ";"
      };
      if (is_defined (response.document[counter].citta)){
        fileContent+= response.document[counter].citta + "\n"
      }else{
        fileContent+= "\n"
      };
       println@Console("done" + counter)()
    };
    writeRequest.filename = "contact.csv";
    writeRequest.content = fileContent;
    writeFile@File(writeRequest)();
    println@Console("done")()
  }]

}
