include "console.iol"
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"

main {
scope (InsertMongoTest){
  install (default => valueToPrettyString@StringUtils(InsertMongoTest)(s);
           println@Console(s)());
           connectValue.host = "localhost";
           connectValue.dbname ="admin";
           connectValue.username = "myUserAdmin";
           connectValue.password = "abc123";
           connectValue.port = 27017;
           connectValue.jsonStringDebug = true;
           connectValue.timeZone = "Europe/Berlin";
           connectValue.logStreamDebug = true;
           valueToPrettyString@StringUtils(connectValue)(s);
           println@Console(s)();
           connect@MongoDB(connectValue)();
           connect@MongoDB(connectValue)();
          listCollection@MongoDB ()(response);
          valueToPrettyString@StringUtils(response)(s);
          println@Console(s)();

          with (requestRole){
               .roleName= "MyFirstRole";
               with(.privilages[0]){
                 .resource.cluster =true;
                 .actions[0]= "addShard"
               };
               with(.privilages[1]){
                 .resource.db ="User";
                 .resource.collection ="";
                 .actions[0]= "find";
                 .actions[1]= "insert"

               };
              .roles[0].role = "read";
              .roles[0].db   = "admin"
               };
          updateRole@MongoDB(requestRole)();
          readRoles@MongoDB()(response);
          valueToPrettyString@StringUtils(response)(s);
          println@Console(s)();
          undef(requestRole);
          requestRole.roleName = "MyFirstRole";
          dropRole@MongoDB(requestRole)();
          readRoles@MongoDB()(response);
          valueToPrettyString@StringUtils(response)(s);
          println@Console(s)()
}

}
