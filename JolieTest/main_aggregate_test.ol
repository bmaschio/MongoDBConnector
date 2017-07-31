include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"


  init{

    connectValue.host = "10.101.50.107";
    connectValue.dbname ="ClientData";
    connectValue.port = 27017;
    connectValue.jsonStringDebug = true;
    connectValue.timeZone = "Europe/Berlin";
    connectValue.username = "prova";
    connectValue.password = "prova";
    connectValue.logStreamDebug = true;
    connect@MongoDB(connectValue)()

  }



main {

q.collection = "Contact";
q.filter[0] = "{$match:{'company.address.reg':'TOSCANA'}} ";
q.filter[1] = "{$group:{ _id : '$company.address.cit', total:{$sum : 1}, prov: 'company.address.prov' }}";


valueToPrettyString@StringUtils (q)(s);
println@Console("q>>>>"+s)();
scope (aggregateScope){
     install (default=>  valueToPrettyString@StringUtils (aggregateScope)(s);
     println@Console("updateScope>>>>"+s)());
     aggregate@MongoDB(q)(responseq)
};
valueToPrettyString@StringUtils (responseq)(s);
println@Console("responseq>>>>"+s)()
}
