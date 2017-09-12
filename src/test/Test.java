/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import jolie.runtime.FaultException;
import jolie.runtime.Value;
import joliex.mongodb.MongoDbConnector;
public class Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FaultException {
        // TODO code application logic here
        
        
        
        MongoDbConnector mongoDbConnector = new MongoDbConnector();
        Value connectValue = Value.create();
        connectValue.getNewChild("host").add(Value.create("localhost"));
        connectValue.getNewChild("dbname").add(Value.create("test"));
        connectValue.getFirstChild("timeZone").add(Value.create("Europe/Berlin"));
        connectValue.getNewChild("port").add(Value.create(27017));
        connectValue.getNewChild("password").add(Value.create("prova"));
        connectValue.getNewChild("username").add(Value.create("prova"));
        connectValue.getNewChild("jsonStringDebug").add(Value.create(Boolean.TRUE));
        mongoDbConnector.connect(connectValue);

       
      
;
 
        Value queryValue = Value.create();
        queryValue.getNewChild("collection").add(Value.create("contact"));      
       // queryValue.getChildren("filter").get(0).add(Value.create("{name : '$name' }"));
       /*queryValue.getChildren("filter").get(0).add(Value.create("{name : {$in : '$name'}}"));
        queryValue.getChildren("filter").get(0).getNewChild("name").add(Value.create("balint"));
        queryValue.getChildren("filter").get(0).getChildren("name").add(Value.create("carlo"));
       */
       queryValue.getChildren("filter").get(0).add(Value.create("{_id : '$_id' }"));
       queryValue.getChildren("filter").get(0).getNewChild("_id").add(Value.create("59b81c891af86d706f5458bf"));
       queryValue.getChildren("filter").get(0).getFirstChild("_id").getNewChild("@type").add(Value.create("ObjectId"));
       Value v = mongoDbConnector.query(queryValue);
         
       System.out.println();
            
    }
    
}
