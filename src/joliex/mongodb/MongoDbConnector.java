/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliex.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import jolie.runtime.JavaService;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import jolie.runtime.CanUseJars;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import org.bson.BsonDocument;
import com.mongodb.client.model.Filters.*;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import jolie.runtime.ValueVector;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 *
 * @author maschio
 */
@CanUseJars({
    "mongo-java-driver-3.2.2.jar",
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
        FindIterable<BsonDocument> iterable = null;
        String collectionName = request.getFirstChild("collection").strValue();
        Value v = Value.create();
        MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
        System.out.println(request.getFirstChild("query").strValue());

        BsonDocument dbObject = BsonDocument.parse(request.getFirstChild("query").strValue());
        prepareQuery(dbObject, request);
        iterable = collection.find(dbObject);

        iterable.forEach(new Block<BsonDocument>() {

            @Override
            public void apply(BsonDocument t) {
                Value queryValue = processQueryRow(t);
                v.getChildren("row").add(queryValue);

            }
        });
        return v;
    }

    @RequestResponse
    public void insert(Value request) {
        String collectionName = request.getFirstChild("collection").strValue();
        BsonDocument bsonDocument = createDocument(request.getFirstChild("document"));

        db.getCollection(collectionName, BsonDocument.class).insertOne(bsonDocument);

    }

    @RequestResponse
    public void delete(Value request) {

    }

    @RequestResponse
    public void update(Value request) {

    }

    private BsonDocument prepareQuery(BsonDocument dbObject, Value request) {
        Set<String> keySet = dbObject.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();
        while (iteratorKeySet.hasNext()) {
            String keyName = iteratorKeySet.next();

            if (dbObject.isString(keyName)) {

                BsonString conditionValue = dbObject.getString(keyName);

                if (conditionValue.getValue().startsWith("$")) {
                    String conditionValueName = conditionValue.getValue().substring(1);
                    if (request.getFirstChild("query").getFirstChild(conditionValueName).isInt()) {
                        BsonInt32 objToInsert = new BsonInt32(request.getFirstChild("query").getFirstChild(conditionValueName).intValue());
                        dbObject.put(keyName, objToInsert);
                    }
                    if (request.getFirstChild("query").getFirstChild(conditionValueName).isDouble()) {
                        BsonDouble objToInsert = new BsonDouble(request.getFirstChild("query").getFirstChild(conditionValueName).doubleValue());
                        dbObject.put(keyName, objToInsert);
                    }
                    if (request.getFirstChild("query").getFirstChild(conditionValueName).isString()) {
                        BsonString objToInsert = new BsonString(request.getFirstChild("query").getFirstChild(conditionValueName).strValue());
                        dbObject.put(keyName, objToInsert);
                    }
                }

            }
            if (dbObject.isArray(keyName)) {
                BsonArray array = dbObject.getArray(keySet);
                ListIterator<BsonValue> listIterator = array.listIterator();

                while (listIterator.hasNext()) {
                    prepareQuery(listIterator.next().asDocument(), request);
                }
            }

            if (dbObject.isDocument(keyName)) {
                BsonDocument conditionObject = dbObject.getDocument(keyName);
                Iterator iteratorMapCondition = conditionObject.keySet().iterator();
                while (iteratorMapCondition.hasNext()) {
                    String conditionName = (String) iteratorMapCondition.next();

                    if (conditionObject.isArray(conditionName)) {
                        BsonArray supportListValueCondition = new BsonArray();
                        BsonArray listValueCondition = conditionObject.asArray();
                        for (int counterCondition = 0; counterCondition < listValueCondition.size(); counterCondition++) {

                            if (listValueCondition.get(counterCondition).isString()) {
                                String conditionValue = listValueCondition.asString().getValue().substring(1);
                                if (request.getFirstChild("query").getFirstChild(conditionValue).isInt()) {

                                    supportListValueCondition.add(new BsonInt32(request.getFirstChild("query").getFirstChild(conditionValue).intValue()));
                                }
                                if (request.getFirstChild("query").getFirstChild(conditionValue).isDouble()) {
                                    supportListValueCondition.add(new BsonDouble(request.getFirstChild("query").getFirstChild(conditionValue).doubleValue()));
                                }
                                if (request.getFirstChild("query").getFirstChild(conditionValue).isString()) {
                                    supportListValueCondition.add(new BsonString(request.getFirstChild("query").getFirstChild(conditionValue).strValue()));
                                }
                            }

                        }
                        conditionObject.append(conditionName, supportListValueCondition);
                    } else {
                        if (conditionObject.get(conditionName).isString()) {
                            String conditionValue = conditionObject.getString(conditionName).getValue().substring(1);

                            if (request.getFirstChild("query").getFirstChild(conditionValue).isInt()) {
                                conditionObject.append(conditionName, new BsonInt32(request.getFirstChild("query").getFirstChild(conditionValue).intValue()));
                            }
                            if (request.getFirstChild("query").getFirstChild(conditionValue).isDouble()) {
                                conditionObject.put(conditionName, new BsonDouble(request.getFirstChild("query").getFirstChild(conditionValue).doubleValue()));

                            }
                            if (request.getFirstChild("query").getFirstChild(conditionValue).isString()) {
                                conditionObject.put(conditionName, new BsonString (request.getFirstChild("query").getFirstChild(conditionValue).strValue()));
                            }
                        }
                    }
                }
       
                dbObject.put(keyName, conditionObject);
            }
        }
        return dbObject;

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

}
