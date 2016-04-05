/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;
import jolie.runtime.Value;
import jolie.runtime.ValuePrettyPrinter;
import joliex.io.ConsoleService;
import joliex.mongodb.MongoDbConnector;
import joliex.util.StringUtils;


/**
 *
 * @author maschio
 */
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
public class Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        DBObject dbObject = (DBObject) JSON.parse("{ qty: { $gt: \"$qty\" } }");
        Set<String> keySet = dbObject.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()){
            String nameKey = iterator.next();
            DBObject obj = (DBObject) dbObject.get(nameKey);
            Map mapCondition = obj.toMap();
            Iterator iteratorMapCondition = mapCondition.keySet().iterator();
            String name = null;
            while (iteratorMapCondition.hasNext()){       
                name = (String) iteratorMapCondition.next();
                mapCondition.put(name, 3);
                System.out.print(name );
            }
            obj.putAll(mapCondition);
            dbObject.put(nameKey, obj);
        }
        
        
        MongoDbConnector mongoDbConnector = new MongoDbConnector();
        Value connectValue = Value.create();
       connectValue.getNewChild("host").add(Value.create("localhost"));
        connectValue.getNewChild("dbname").add(Value.create("prova"));
        connectValue.getNewChild("port").add(Value.create(27017));
        mongoDbConnector.connect(connectValue);

       
      
        Value InsertValue = Value.create();
        InsertValue.getNewChild("table").add(Value.create("prove"));
        InsertValue.getNewChild("data");
        InsertValue.getFirstChild("data").getChildren("nome").add(Value.create("Marco"));
        InsertValue.getFirstChild("data").getChildren("Telefoni").add(Value.create("231231231"));
        InsertValue.getFirstChild("data").getChildren("Telefoni").add(Value.create("131231231"));
        mongoDbConnector.insert(InsertValue);
        System.out.println("ciao");
    }
    
}
