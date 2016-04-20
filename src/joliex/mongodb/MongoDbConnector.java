/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliex.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import jolie.runtime.JavaService;
import com.mongodb.Block;
import com.mongodb.DBObject;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import java.util.Set;
import jolie.runtime.CanUseJars;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;
import jolie.runtime.embedding.RequestResponse;
import org.bson.BSONObject;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

/**
 *
 * @author maschio
 */
@CanUseJars({
    "mongo-java-driver-3.2.0.jar",
    "mongo-java-logging-0.5.3.jar"

})
public class MongoDbConnector extends JavaService {

    private String username;
    private String password;
    private String dbname;
    private String host;
    private int port;
    private MongoClient mongoClient;
    private MongoDatabase db;
    private MongoClientOptions mongoClientOptions;

    @RequestResponse
    public void connect(Value request) {
        host = request.getFirstChild("host").strValue();
        port = request.getFirstChild("port").intValue();
        dbname = request.getFirstChild("dbname").strValue();
        mongoClient = new MongoClient(host, port);
        db = mongoClient.getDatabase(dbname);
    }

    @RequestResponse

    public Value query(Value request) {
        FindIterable<Document> iterable = null;
        String collectionName = request.getFirstChild("collection").strValue();
        Value v = Value.create();
        MongoCollection<Document> collection = db.getCollection(collectionName);
        if (request.hasChildren("query")) {
            System.out.println(request.getFirstChild("query").strValue());
            BSONObject dbObject = (BSONObject) JSON.parse(request.getFirstChild("query").strValue());
            iterable = collection.find((Bson) prepareQuery(dbObject, request));

        } else {
            iterable = collection.find();
        };

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                Value queryValue = processQueryRow(document);
                v.getChildren("row").add(queryValue);
            }
        });
        return v;
    }

    @RequestResponse
    public void insert(Value request) {
        String collectionName = request.getFirstChild("collection").strValue();
        Document document = createDocument(request.getFirstChild("data"));
        db.getCollection(collectionName).insertOne(document);
    }
   @RequestResponse
   public void delete (Value request){
  
        String table = request.getFirstChild("table").strValue();
        Value v = Value.create();
        MongoCollection<Document> collection = db.getCollection(table);
        BSONObject dbObject = (BSONObject) JSON.parse(request.getFirstChild("query").strValue());
        collection.deleteMany((Bson) prepareQuery(dbObject, request));
   }
   
   @RequestResponse
   public void update (Value request ){
        String table = request.getFirstChild("table").strValue();
        Value v = Value.create();
        MongoCollection<Document> collection = db.getCollection(table);
        BSONObject dbObject = (BSONObject) JSON.parse(request.getFirstChild("query").strValue());
        Document document = createDocument(request.getFirstChild("data"));
        collection.updateMany((Bson) prepareQuery(dbObject, request),document);
   
   }
    private BSONObject prepareQuery(BSONObject dbObject, Value request) {

        Set<String> keySet = dbObject.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();
        while (iteratorKeySet.hasNext()) {
            String nameKey = iteratorKeySet.next();
            if (dbObject.get(nameKey) instanceof String) {

                String conditionValue = (String) dbObject.get(nameKey);
                
                if (conditionValue.startsWith("$")) {
                    conditionValue = conditionValue.substring(1);
                    if (request.getFirstChild("query").getFirstChild(conditionValue).isInt()) {
                        dbObject.put(nameKey, request.getFirstChild("query").getFirstChild(conditionValue).intValue());
                    }
                    if (request.getFirstChild("query").getFirstChild(conditionValue).isDouble()) {
                        dbObject.put(nameKey, request.getFirstChild("query").getFirstChild(conditionValue).doubleValue());
                    }
                    if (request.getFirstChild("query").getFirstChild(conditionValue).isString()) {
                        dbObject.put(nameKey, request.getFirstChild("query").getFirstChild(conditionValue).strValue());
                    }
                }

            }
            
            if (dbObject.get(nameKey) instanceof BasicDBList) {
                BasicDBList basicDbList = (BasicDBList) dbObject.get(nameKey);
                Iterator<Object> listIterator = basicDbList.iterator();
                while (listIterator.hasNext()) {
                    prepareQuery((BSONObject) listIterator.next(), request);
                }
            }

            if (dbObject.get(nameKey) instanceof BasicDBObject) {
                DBObject conditionObject = (DBObject) dbObject.get(nameKey);
                Map conditionsMap = conditionObject.toMap();
                Iterator iteratorMapCondition = conditionsMap.keySet().iterator();
                while (iteratorMapCondition.hasNext()) {
                    String conditionName = (String) iteratorMapCondition.next();
                     
                    if (conditionsMap.get(conditionName) instanceof BasicDBList) {
                        BasicDBList supportListValueCondition = new BasicDBList();
                        BasicDBList listValueCondition = (BasicDBList) conditionsMap.get(conditionName);
                        Iterator<Object> listValueConditionIterator = listValueCondition.iterator();
                        while (listValueConditionIterator.hasNext()) {
                            String conditionValue = (String) listValueConditionIterator.next();
                            conditionValue = conditionValue.substring(1);
                            if (request.getFirstChild("query").getFirstChild(conditionValue).isInt()) {
                                supportListValueCondition.add(request.getFirstChild("query").getFirstChild(conditionValue).intValue());
                            }
                            if (request.getFirstChild("query").getFirstChild(conditionValue).isDouble()) {
                                supportListValueCondition.add(request.getFirstChild("query").getFirstChild(conditionValue).doubleValue());
                            }
                            if (request.getFirstChild("query").getFirstChild(conditionValue).isString()) {
                                supportListValueCondition.add(request.getFirstChild("query").getFirstChild(conditionValue).strValue());
                            }

                        }
                        conditionsMap.put(conditionName, supportListValueCondition);
                    } else {
                        String conditionValue = (String) conditionsMap.get(conditionName);
                        conditionValue = conditionValue.substring(1);
                        if (request.getFirstChild("query").getFirstChild(conditionValue).isInt()) {
                            conditionsMap.put(conditionName, request.getFirstChild("query").getFirstChild(conditionValue).intValue());
                        }
                        if (request.getFirstChild("query").getFirstChild(conditionValue).isDouble()) {
                            conditionsMap.put(conditionName, request.getFirstChild("query").getFirstChild(conditionValue).doubleValue());
                        }
                        if (request.getFirstChild("query").getFirstChild(conditionValue).isString()) {
                            conditionsMap.put(conditionName, request.getFirstChild("query").getFirstChild(conditionValue).strValue());
                        }
                    }
                }
                conditionObject.putAll(conditionsMap);
                dbObject.put(nameKey, conditionObject);
            }
        }
        return dbObject;
    }

    private Document createDocument(Value request) {
        Document doc = new Document();

        Map<String, ValueVector> children = request.children();
        Set<Map.Entry<String, ValueVector>> childrenSet = children.entrySet();
        Iterator<Map.Entry<String, ValueVector>> iterator = childrenSet.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ValueVector> entry = iterator.next();
            ValueVector valueVector = entry.getValue();
            if (!valueVector.isEmpty()) {
                for (int counterValueVector = 0; counterValueVector < valueVector.size(); counterValueVector++) {
                    if (valueVector.get(counterValueVector).hasChildren()) {
                        doc.append(entry.getKey(), createDocument(valueVector.get(counterValueVector)));
                    }
                    if (valueVector.get(counterValueVector).isBool()) {
                        doc.append(entry.getKey(), valueVector.get(counterValueVector).boolValue());
                    }
                    if (valueVector.get(counterValueVector).isInt()) {
                        doc.append(entry.getKey(), valueVector.get(counterValueVector).intValue());
                    }
                    if (valueVector.get(counterValueVector).isDouble()) {
                        doc.append(entry.getKey(), valueVector.get(counterValueVector).doubleValue());
                    }
                    if (valueVector.get(counterValueVector).isString()) {
                        doc.append(entry.getKey(), valueVector.get(counterValueVector).strValue());
                    }
                }
            } else {
                System.out.println("Empty");
            }
        }

        return doc;
    }

    private Value processQueryRow(Document document) {
        Value v = Value.create();
        Set<String> keySet = document.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();

        while (iteratorKeySet.hasNext()) {
            String nameField = iteratorKeySet.next();
            if (document.get(nameField) instanceof String) {
                v.getChildren(nameField).add(Value.create(document.getString(nameField)));
            } else if (document.get(nameField) instanceof Integer) {
                v.getChildren(nameField).add(Value.create(document.getInteger(nameField)));
            } else if (document.get(nameField) instanceof Double) {
                v.getChildren(nameField).add(Value.create(document.getDouble(nameField)));
            } else if (document.get(nameField) instanceof Date) {
                Date date = document.getDate(nameField);
                v.getChildren(nameField).add(Value.create(date.toString()));

            } else if (document.get(nameField) instanceof Document) {
                v.getChildren(nameField).add(processQueryRow(Document.class
                        .cast(document.get(nameField))));
            } else if (document.get(nameField).getClass().toString().equals("class java.util.ArrayList")) {

                ArrayList<Object> array = ArrayList.class
                        .cast(document.get(nameField));
                Iterator<Object> iteratorArray = array.iterator();

                while (iteratorArray.hasNext()) {
                    Object obj = iteratorArray.next();

                    if (obj instanceof String) {
                        v.getChildren(nameField).add(Value.create(String.class.cast(obj)));
                    } else if (obj instanceof Integer) {
                        v.getChildren(nameField).add(Value.create(Integer.class.cast(obj)));
                    } else if (obj instanceof Double) {
                        v.getChildren(nameField).add(Value.create(Double.class.cast(obj)));
                    } else if (obj instanceof Date) {
                        Date date = Date.class.cast(obj);
                        v.getChildren(nameField).add(Value.create(date.toString()));
                    } else if (obj instanceof Document) {
                        v.getChildren(nameField).add(processQueryRow(Document.class.cast(obj)));
                    }
                }

            } else if (document.get(nameField) instanceof ObjectId) {
                ObjectId objId;
                objId = (ObjectId) document.get(nameField);
                v.add(Value.create(objId.toHexString()));

            }

        }

        return v;
    }

}
