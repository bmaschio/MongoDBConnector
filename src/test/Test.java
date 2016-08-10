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
        connectValue.getNewChild("dbname").add(Value.create("prova"));
        connectValue.getNewChild("port").add(Value.create(27017));
        connectValue.getNewChild("jsonStringDebug").add(Value.create(Boolean.TRUE));
        mongoDbConnector.connect(connectValue);

       
      
       Value InsertValue = Value.create();
        InsertValue.getNewChild("collection").add(Value.create("prove"));
        InsertValue.getNewChild("document");
        InsertValue.getFirstChild("document").getChildren("nome").add(Value.create("Mark"));
        InsertValue.getFirstChild("document").getChildren("cognome").add(Value.create("Green"));
        InsertValue.getFirstChild("document").getChildren("age").add(Value.create(37));
        
        InsertValue.getFirstChild("document").getNewChild("spese").getChildren("ammount").add(Value.create(10.2));
        InsertValue.getFirstChild("document").getNewChild("spese").getChildren("ammount").add(Value.create(10.3));
 //       mongoDbConnector.insert(InsertValue);
 
        Value queryValue = Value.create();
        queryValue.getNewChild("collection").add(Value.create("CustomerSales"));
        
        
       queryValue.getChildren("filter").get(0).add(Value.create("{$group:{ _id : '$name', total:{$sum : 1}}}"));
//
       // queryValue.getFirstChild("query").getChildren("ammount").add(Value.create(30.0));

         Value v = mongoDbConnector.aggregate(queryValue);
         System.out.println();
            
    }
    
}
