/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliex.mongodb;

import com.mongodb.Block;
import jolie.runtime.JavaService;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import jolie.runtime.CanUseJars;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import org.bson.BsonDocument;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import jolie.runtime.FaultException;
import jolie.runtime.ValueVector;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;

/**
 *
 * @author maschio
 */
@CanUseJars({
    "mongo-java-driver-3.2.2.jar",
    "mongodb-driver-3.2.2.jar",
    "mongodb-driver-core-3.2.2.jar"

})
public class MongoDbConnector extends JavaService {

    private String username;
    private String password;
    private String dbname;
    private String host;
    private int port;
    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private MongoClientOptions mongoClientOptions;
    private static Logger log;
    private static boolean jsonDebuger;

    @RequestResponse
    public void connect(Value request) throws FaultException {
        try {
            host = request.getFirstChild("host").strValue();
            port = request.getFirstChild("port").intValue();
            dbname = request.getFirstChild("dbname").strValue();
            log = Logger.getLogger("org.mongodb.driver");
            log.setLevel(Level.OFF);
            if (request.hasChildren("jsonStringDebug")) {
                jsonDebuger = request.getFirstChild("jsonStringDebug").boolValue();
            }
            mongoClient = new MongoClient(host, port);
            db = mongoClient.getDatabase(dbname);
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        }
    }

    @RequestResponse
    public Value query(Value request) throws FaultException {
        Value v = Value.create();
        FindIterable<BsonDocument> iterable;
        try {
            String collectionName = request.getFirstChild("collection").strValue();
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            if (request.hasChildren("filter")) {
                BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
                prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
                printlnJson("Query filter", bsonQueryDocument);
                iterable = collection.find(bsonQueryDocument);
            } else {
                iterable = collection.find();
            }
            iterable.forEach(new Block<BsonDocument>() {
                @Override
                public void apply(BsonDocument t) {
                    Value queryValue = processQueryRow(t);
                    printlnJson("Query document", t);
                    v.getChildren("document").add(queryValue);
                }
            });
          
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }
        
        return v;
    }

    @RequestResponse
    public void insert(Value request) throws FaultException {
        try {

            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonDocument = createDocument(request.getFirstChild("document"));
            printlnJson("insert document", bsonDocument);
            db.getCollection(collectionName, BsonDocument.class).insertOne(bsonDocument);

        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }

    }

    @RequestResponse
    public void delete(Value request) throws FaultException {
        try {
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Delete filter", bsonQueryDocument);
            db.getCollection(collectionName, BsonDocument.class).deleteMany(bsonQueryDocument);
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }
    }

    @RequestResponse
    public void update(Value request) throws FaultException {
        try {
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Update filter", bsonQueryDocument);
            BsonDocument bsonDocument = BsonDocument.parse(request.getFirstChild("documentUpdate").strValue());
            prepareBsonQueryData(bsonDocument, request.getFirstChild("documentUpdate"));
            printlnJson("Update documentUpdate", bsonQueryDocument);
            db.getCollection(collectionName, BsonDocument.class).updateMany(bsonQueryDocument, bsonDocument);
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }

    }

    @RequestResponse
    public Value aggregate(Value request) {
        Value v = Value.create();
        ArrayList<BsonDocument> bsonAggreagationDocuments = new ArrayList<>();
        String collectionName = request.getFirstChild("collection").strValue();
        for (int counter = 0; counter < request.getChildren("filter").size(); counter++) {
            BsonDocument bsonAggregationDocument = BsonDocument.parse(request.getChildren("filter").get(counter).strValue());
            prepareBsonQueryData(bsonAggregationDocument, request.getChildren("filter").get(counter));
            printlnJson("Aggregate filter", bsonAggregationDocument);
            bsonAggreagationDocuments.add(bsonAggregationDocument);
        }
        AggregateIterable<BsonDocument> aggregation = db.getCollection(collectionName).aggregate(bsonAggreagationDocuments, BsonDocument.class).allowDiskUse(Boolean.TRUE);

        aggregation.forEach(new Block<BsonDocument>() {
            @Override
            public void apply(BsonDocument t) {
                printlnJson("Aggregate Document", t);
                Value queryValue = processQueryRow(t);
                v.getChildren("document").add(queryValue);
            }
        });

        return v;
    }

    private BsonDocument prepareBsonQueryData(BsonDocument bsonQueryDocument, Value request) {
        Set<String> keySet = bsonQueryDocument.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();
        while (iteratorKeySet.hasNext()) {
            String keyName = iteratorKeySet.next();

            if (bsonQueryDocument.isString(keyName)) {

                BsonString conditionValue = bsonQueryDocument.getString(keyName);

                if (conditionValue.getValue().startsWith("$")) {
                    String conditionValueName = conditionValue.getValue().substring(1);

                    if (request.getFirstChild(conditionValueName).isInt()) {
                        BsonInt32 objToInsert = new BsonInt32(request.getFirstChild(conditionValueName).intValue());
                        bsonQueryDocument.put(keyName, objToInsert);
                    }
                    if (request.getFirstChild(conditionValueName).isDouble()) {
                        BsonDouble objToInsert = new BsonDouble(request.getFirstChild(conditionValueName).doubleValue());
                        bsonQueryDocument.put(keyName, objToInsert);
                    }
                    if (request.getFirstChild(conditionValueName).isString()) {
                        if (request.getFirstChild(conditionValueName).hasChildren("@Date")) {
                            //BsonDateTime objToInsert = new BsonDateTime(port)
                        } else {
                            BsonString objToInsert = new BsonString(request.getFirstChild(conditionValueName).strValue());
                            bsonQueryDocument.put(keyName, objToInsert);
                        }
                    }
                    if (request.getFirstChild(conditionValueName).hasChildren()) {

                        bsonQueryDocument.put(keyName, createDocument(request.getFirstChild(conditionValueName)));
                    }
                }

            }
            if (bsonQueryDocument.isArray(keyName)) {
                BsonArray array = bsonQueryDocument.getArray(keySet);
                ListIterator<BsonValue> listIterator = array.listIterator();

                while (listIterator.hasNext()) {
                    prepareBsonQueryData(listIterator.next().asDocument(), request);
                }
            }

            if (bsonQueryDocument.isDocument(keyName)) {
                BsonDocument conditionObject = bsonQueryDocument.getDocument(keyName);
                Iterator iteratorMapCondition = conditionObject.keySet().iterator();
                while (iteratorMapCondition.hasNext()) {
                    String conditionName = (String) iteratorMapCondition.next();

                    if (conditionObject.isArray(conditionName)) {
                        BsonArray supportListValueCondition = new BsonArray();
                        BsonArray listValueCondition = conditionObject.asArray();
                        for (int counterCondition = 0; counterCondition < listValueCondition.size(); counterCondition++) {

                            if (listValueCondition.get(counterCondition).isString()) {
                                String conditionValue = listValueCondition.asString().getValue().substring(1);
                                if (request.getFirstChild(conditionValue).isInt()) {

                                    supportListValueCondition.add(new BsonInt32(request.getFirstChild(conditionValue).intValue()));
                                }
                                if (request.getFirstChild(conditionValue).isDouble()) {
                                    supportListValueCondition.add(new BsonDouble(request.getFirstChild(conditionValue).doubleValue()));
                                }
                                if (request.getFirstChild(conditionValue).isString()) {
                                    supportListValueCondition.add(new BsonString(request.getFirstChild(conditionValue).strValue()));
                                }
                            }

                        }
                        conditionObject.append(conditionName, supportListValueCondition);
                    } else {
                        if (conditionObject.get(conditionName).isString()) {
                            String conditionValue = conditionObject.getString(conditionName).getValue().substring(1);

                            if (request.getFirstChild(conditionValue).isInt()) {
                                conditionObject.append(conditionName, new BsonInt32(request.getFirstChild(conditionValue).intValue()));
                            }
                            if (request.getFirstChild(conditionValue).isDouble()) {
                                conditionObject.put(conditionName, new BsonDouble(request.getFirstChild(conditionValue).doubleValue()));

                            }
                            if (request.getFirstChild(conditionValue).isString()) {
                                conditionObject.put(conditionName, new BsonString(request.getFirstChild(conditionValue).strValue()));
                            }

                            if (request.getFirstChild(conditionValue).hasChildren()) {

                                conditionObject.put(keyName, createDocument(request.getFirstChild(conditionValue)));
                            }
                        }
                    }
                }

                bsonQueryDocument.put(keyName, conditionObject);
            }
        }
        return bsonQueryDocument;

    }

    private BsonDocument createDocument(Value request) {
        BsonDocument bsonDocument = new BsonDocument();
        Map<String, ValueVector> children = request.children();
        Set<Map.Entry<String, ValueVector>> childrenSet = children.entrySet();
        Iterator<Map.Entry<String, ValueVector>> iterator = childrenSet.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ValueVector> entry = iterator.next();
            ValueVector valueVector = entry.getValue();
            if (!valueVector.isEmpty()) {
                BsonArray bsonArray = new BsonArray();
                for (int counterValueVector = 0; counterValueVector < valueVector.size(); counterValueVector++) {

                    if (valueVector.get(counterValueVector).hasChildren()) {

                        bsonArray.add(counterValueVector, createDocument(valueVector.get(counterValueVector)));

                    }
                    if (valueVector.get(counterValueVector).isInt()) {
                        BsonInt32 bsonObj = new BsonInt32(valueVector.get(counterValueVector).intValue());
                        if (valueVector.size() == 1) {
                            bsonDocument.append(entry.getKey(), bsonObj);
                        } else {
                            bsonArray.add(counterValueVector, bsonObj);
                        }
                    }
                    if (valueVector.get(counterValueVector).isDouble()) {
                        BsonDouble bsonObj = new BsonDouble(valueVector.get(counterValueVector).doubleValue());
                        if (valueVector.size() == 1) {
                            bsonDocument.append(entry.getKey(), bsonObj);
                        } else {
                            bsonArray.add(counterValueVector, bsonObj);
                        }
                    }
                    if (valueVector.get(counterValueVector).isString()) {
                        BsonString bsonObj = new BsonString(valueVector.get(counterValueVector).strValue());
                        if (valueVector.size() == 1) {
                            bsonDocument.append(entry.getKey(), bsonObj);
                        } else {
                            bsonArray.add(counterValueVector, bsonObj);
                        }
                    }
                }
                if (!bsonArray.isEmpty()) {
                    bsonDocument.append(entry.getKey(), bsonArray);
                }
            } else {
                System.out.println("Empty");
            }

        }
        return bsonDocument;
    }

    private Value processQueryRow(BsonDocument document) {
        Value v = Value.create();
        Set<String> keySet = document.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();

        while (iteratorKeySet.hasNext()) {
            String nameField = iteratorKeySet.next();
            if (document.isString(nameField)) {
                v.getChildren(nameField).add(Value.create(document.getString(nameField).getValue()));
            } else if (document.isInt32(nameField)) {
                v.getChildren(nameField).add(Value.create(document.getInt32(nameField).getValue()));
            } else if (document.isDouble(nameField)) {
                v.getChildren(nameField).add(Value.create(document.getDouble(nameField).getValue()));
            } else if (document.isDateTime(nameField)) {
                Date date = new Date(document.getDateTime(nameField).getValue());
                v.getChildren(nameField).add(Value.create(date.toString()));
            } else if (document.isDocument(nameField)) {
                v.getChildren(nameField).add(processQueryRow(document.getDocument(nameField)));
            } else if (document.isArray(nameField)) {

                List<BsonValue> array = document.getArray(nameField).getValues();
                Iterator<BsonValue> iteratorArray = array.iterator();

                while (iteratorArray.hasNext()) {
                    Object obj = iteratorArray.next();

                    if (obj instanceof BsonString) {
                        BsonString bsonObj = (BsonString) obj;
                        v.getChildren(nameField).add(Value.create(bsonObj.getValue()));
                    } else if (obj instanceof BsonInt32) {
                        BsonInt32 bsonObj = (BsonInt32) obj;
                        v.getChildren(nameField).add(Value.create(bsonObj.getValue()));
                    } else if (obj instanceof BsonDouble) {
                        BsonDouble bsonObj = (BsonDouble) obj;
                        v.getChildren(nameField).add(Value.create(bsonObj.getValue()));
                    } else if (obj instanceof BsonDateTime) {
                        BsonDateTime date = BsonDateTime.class.cast(obj);
                        v.getChildren(nameField).add(Value.create(date.toString()));
                    } else if (obj instanceof BsonDocument) {
                        BsonDocument bsonObj = (BsonDocument) obj;
                        v.getChildren(nameField).add(processQueryRow(bsonObj));
                    }
                }

            } else if (document.get(nameField).isObjectId()) {
                BsonObjectId objId;
                objId = document.getObjectId(nameField);
                v.add(Value.create(objId.getValue().toHexString()));

            }

        }

        return v;

    }
private void printlnJson (String level , BsonDocument bsonDocument ){
    if (jsonDebuger){
        System.out.println(level +" "+ bsonDocument.toJson());
    }
}
}
