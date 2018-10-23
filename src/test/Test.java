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
        connectValue.getNewChild("dbname").add(Value.create("butiko"));
        connectValue.getFirstChild("timeZone").add(Value.create("Europe/Berlin"));
        connectValue.getNewChild("port").add(Value.create(27017));
        connectValue.getNewChild("password").add(Value.create("5D6z55MUBMYEH5cc6tZs"));
        connectValue.getNewChild("username").add(Value.create("butiko"));
        connectValue.getNewChild("jsonStringDebug").add(Value.create(Boolean.TRUE));
        mongoDbConnector.connect(connectValue);
        Value findValue = Value.create();
        findValue.getNewChild( "collection").setValue( "Shops");
        findValue.getNewChild("filter").add(Value.create("{ $match: { $text: { $search: '$tags' } ,location: {$geoWithin: {$centerSphere: ['$pos', 5000] }}}}"));
	
		//inline.getNew
        //findValue.getFirstChild("filter").getNewChild("centerSphere").add(Value.get);
		findValue.getFirstChild("filter").getChildren("pos").add(Value.create( new Double(44.4875055)));
		findValue.getFirstChild("filter").getChildren("pos").add(Value.create( new Double(11.36450979)));
        findValue.getFirstChild("filter").getChildren("tags").add(Value.create("bar"));
        mongoDbConnector.aggregate(findValue);
         
       System.out.println();
            
    }
    
}
