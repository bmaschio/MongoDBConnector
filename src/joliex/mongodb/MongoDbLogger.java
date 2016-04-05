/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliex.mongodb;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 *
 * @author maschio
 */
public class MongoDbLogger extends Formatter{

    @Override
    public String format(LogRecord lr) {
        System.out.println ("ciao");
        System.out.println (lr.getMessage()); //To change body of generated methods, choose Tools | Templates.
        return lr.getLoggerName();
    }
    
}
