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
        connectValue.getNewChild("host").add(Value.create("10.101.50.107"));
        connectValue.getNewChild("dbname").add(Value.create("ClientData"));
        connectValue.getFirstChild("timeZone").add(Value.create("Europe/Berlin"));
        connectValue.getNewChild("port").add(Value.create(27017));
        connectValue.getNewChild("password").add(Value.create("prova"));
        connectValue.getNewChild("username").add(Value.create("prova"));
        connectValue.getNewChild("jsonStringDebug").add(Value.create(Boolean.TRUE));
        mongoDbConnector.connect(connectValue);

       
      
;
 
        Value queryValue = Value.create();
        queryValue.getNewChild("collection").add(Value.create("Contact"));
        
        
       queryValue.getChildren("filter").get(0).add(Value.create("{$group:{ _id : '$company.address.prov', total:{$sum : 1}}}"));
//
       // queryValue.getFirstChild("query").getChildren("ammount").add(Value.create(30.0));

         Value v = mongoDbConnector.aggregate(queryValue);
         System.out.println();
            
    }
    
}
