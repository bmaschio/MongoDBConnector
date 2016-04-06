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
import org.bson.BSONObject;
public class Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        
        
        
        MongoDbConnector mongoDbConnector = new MongoDbConnector();
        Value connectValue = Value.create();
        connectValue.getNewChild("host").add(Value.create("localhost"));
        connectValue.getNewChild("dbname").add(Value.create("prova"));
        connectValue.getNewChild("port").add(Value.create(27017));
        mongoDbConnector.connect(connectValue);

       
      
 /*       Value InsertValue = Value.create();
        InsertValue.getNewChild("table").add(Value.create("prove"));
        InsertValue.getNewChild("data");
        InsertValue.getFirstChild("data").getChildren("nome").add(Value.create("Mark"));
        InsertValue.getFirstChild("data").getChildren("cognome").add(Value.create("Green"));
        InsertValue.getFirstChild("data").getChildren("age").add(Value.create(37));
        mongoDbConnector.insert(InsertValue);
 */
        Value queryValue = Value.create();
        queryValue.getNewChild("table").add(Value.create("prove"));
        queryValue.getNewChild("query").add(Value.create("{nome: \"$nome\"}"));
        queryValue.getFirstChild("query").getChildren("$nome").add(Value.create("Tom"));
        mongoDbConnector.query(queryValue);
            
    }
    
}
