include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "file.iol"

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

main{
  q.collection = "mycollection";
  q.filter = "{ surname: '$surname'}";
  q.filter.surname = "Green";
  query@MongoDB(q)(result);
  undef (q);
  undef( result );

  q.collection = "mycollection";
  q.filter = "{ $or : [{ammount:{$lt: '$lt'}},{ammount:{$gt: '$gt'}}]}";
  q.filter.gt = 13.0;
  q.filter.lt = 11.0;
  query@MongoDB(q)(result);
  undef (q);
  undef( result );

  q.collection = "mycollection";
  q.filter = "{ $and : [{ammount:{$lt: '$lt'}},{ammount:{$gt: '$gt'}}]}";
  q.filter.gt = 10.0;
  q.filter.lt = 14.0;
  query@MongoDB(q)(result);
  undef (q);
  undef( result );

  q.collection = "mycollection";
  q.filter = "{ 'address.city' : 'York'}";
  query@MongoDB(q)(result);
  undef (q);
  undef( result );


  initialDate = 1502469661159L;

  q.collection = "mycollection";
  q.filter = "{ dateInsert : {$gt:'$dateInsert'}}";
  q.filter.dateInsert = initialDate;
  q.filter.dateInsert.("@type") = "Date";
  query@MongoDB(q)(result);
  undef (q);
  undef( result )


}
