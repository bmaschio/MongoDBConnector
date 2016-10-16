/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliex.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBObject;
import jolie.runtime.JavaService;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import java.util.ArrayList;
import jolie.runtime.CanUseJars;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import org.bson.BsonDocument;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import jolie.runtime.FaultException;
import jolie.runtime.ValueVector;
import jolie.runtime.correlation.CorrelationError;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.joda.time.DateTimeZone;

/**
 *
 * @author maschio
 */
@CanUseJars({
    "mongo-java-driver-3.2.2.jar",
    "mongodb-driver-3.2.2.jar",
    "mongodb-driver-core-3.2.2.jar",
    "joda-time-2.4.jar"
})
public class MongoDbConnector extends JavaService {

    private String username;
    private String password;
    private String dbname;
    private String host;
    private int port;
    private String timeZone;
    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private MongoClientOptions mongoClientOptions;
    private static Logger log;
    private static boolean jsonDebuger;
    private static boolean is64;
    private DateTimeZone zone;
    private static boolean logStream;
    private String logString;

    @RequestResponse
    public void connect(Value request) throws FaultException {
        try {
            host = request.getFirstChild("host").strValue();
            port = request.getFirstChild("port").intValue();
            dbname = request.getFirstChild("dbname").strValue();
            timeZone = request.getFirstChild("timeZone").strValue();
            password = request.getFirstChild("password").strValue();
            username = request.getFirstChild("username").strValue();
            log = Logger.getLogger("org.mongodb.driver");
            log.setLevel(Level.OFF);
            if (request.hasChildren("jsonStringDebug")) {
                jsonDebuger = request.getFirstChild("jsonStringDebug").boolValue();
            }
            if (request.hasChildren("logStreamDebug")) {
                logStream = request.getFirstChild("logStreamDebug").boolValue();
                logString = "Processing Steps at " + System.currentTimeMillis();
            }
            if (System.getProperty("os.arch").contains("64")) {
                is64 = true;
            } else {
                is64 = false;
            }
            ServerAddress serverAddress = new ServerAddress(host, port);
            ArrayList<MongoCredential> credentials = new ArrayList();
            MongoCredential credential = MongoCredential.createCredential(username, dbname, password.toCharArray());
            credentials.add(credential);
            if (null != mongoClient) {
                    System.out.println("recovering client");
                    db = mongoClient.getDatabase(dbname);
            }else{
                    mongoClient = new MongoClient(serverAddress, credentials);
                    db = mongoClient.getDatabase(dbname);
            }
            
            zone = DateTimeZone.forID(timeZone);
            DateTimeZone.setDefault(zone);

        } catch (MongoException ex) {
            throw new FaultException("LoginConnection", ex);
        }
    }
    
    @RequestResponse void close (Value request){
       mongoClient.close();
    }

    @RequestResponse
    public Value query(Value request) throws FaultException {
        Value v = Value.create();
        FindIterable<BsonDocument> iterable;
        iterable = null;
        try {

            String collectionName = request.getFirstChild("collection").strValue();
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            if (request.hasChildren("filter")) {
                BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
                prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
                printlnJson("Query filter", bsonQueryDocument);
                if (request.hasChildren("sort") && request.hasChildren("limit")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    int limitQuery = request.getFirstChild("limit").intValue();
                    iterable = collection.find(bsonQueryDocument).sort(bsonSortDocument).limit(limitQuery);
                }
                if (request.hasChildren("sort") && !request.hasChildren("limit")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    iterable = collection.find(bsonQueryDocument).sort(bsonSortDocument);
                }
                if (!request.hasChildren("sort") && request.hasChildren("limit")) {
                    int limitQuery = request.getFirstChild("limit").intValue();
                    iterable = collection.find(bsonQueryDocument).limit(limitQuery);
                }
                if (!request.hasChildren("sort") && !request.hasChildren("limit")) {

                    iterable = collection.find(bsonQueryDocument);
                }

            } else {

                if (request.hasChildren("sort") && request.hasChildren("limit")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    int limitQuery = request.getFirstChild("limit").intValue();
                    iterable = collection.find().sort(bsonSortDocument).limit(limitQuery);
                }
                if (request.hasChildren("sort") && !request.hasChildren("limit")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    iterable = collection.find().sort(bsonSortDocument);
                }
                if (!request.hasChildren("sort") && request.hasChildren("limit")) {
                    int limitQuery = request.getFirstChild("limit").intValue();
                    iterable = collection.find().limit(limitQuery);
                }
                if (!request.hasChildren("sort") && !request.hasChildren("limit")) {

                    iterable = collection.find();
                }

            }
            iterable.forEach(new Block<BsonDocument>() {
                @Override
                public void apply(BsonDocument t) {
                    Value queryValue = processQueryRow(t);
                    printlnJson("Query document", t);
                    v.getChildren("document").add(queryValue);
                }
            });
            showLog();
        } catch (MongoException ex) {
            showLog();
            throw new FaultException("MongoException", ex);

        } catch (JsonParseException ex) {
            showLog();
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
            printlnJson("Update documentUpdate", bsonDocument);
            prepareBsonQueryData(bsonDocument, request.getFirstChild("documentUpdate"));
            printlnJson("Update documentUpdate", bsonDocument);
            showLog();
            db.getCollection(collectionName, BsonDocument.class).updateMany(bsonQueryDocument, bsonDocument);
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }

    }

    @RequestResponse
    public Value listCollection() {
        Value v = Value.create();
        MongoIterable<String> listCollectionNames = db.listCollectionNames();
        MongoCursor<String> iteratorListCollectionNames = listCollectionNames.iterator();
        int counterCollection = 0;
        while (iteratorListCollectionNames.hasNext()) {
            String collection = iteratorListCollectionNames.next();
            v.getChildren("collection").get(counterCollection).add(Value.create(collection));
            counterCollection++;
        }
        return v;
    }

    @RequestResponse
    public Value listDB() {
        Value v = Value.create();
        MongoIterable<String> databaseNames = mongoClient.listDatabaseNames();
        MongoCursor<String> databaseNamesIterator = databaseNames.iterator();
        int counterDatabase = 0;
        while (databaseNamesIterator.hasNext()) {
            v.getChildren("db").get(counterDatabase).add(Value.create(databaseNamesIterator.next()));

        }
        return v;
    }

    @RequestResponse
    public Value createRole(Value request) {

        Value v = Value.create();
        DBObject createRoleObj = new BasicDBObject();
        createRoleObj.put("createRole", request.getFirstChild("roleName").strValue());
        ArrayList<DBObject> privilages = new ArrayList();
        for (int counterPrivilages = 0; counterPrivilages < request.getChildren("privilages").size(); counterPrivilages++) {
            DBObject privilageObj = new BasicDBObject();

            DBObject resourceObj = new BasicDBObject();
            Map<String, ValueVector> resourceMap = request.getChildren("privilages").get(counterPrivilages).getFirstChild("resource").children();
            Set<String> keyResource = resourceMap.keySet();
            Iterator<String> keyResourceIterator = keyResource.iterator();
            while (keyResourceIterator.hasNext()) {
                String resourceName = keyResourceIterator.next();

                ValueVector resourceData = resourceMap.get(resourceName);
                if (resourceData.get(0).isBool()) {
                    resourceObj.put(resourceName, resourceData.get(0).boolValue());
                }
                if (resourceData.get(0).isString()) {
                    resourceObj.put(resourceName, resourceData.get(0).strValue());
                }
            }

            privilageObj.put("resource", resourceObj);
            ArrayList<String> actionsObject = new ArrayList();
            for (int counterActions = 0; counterActions < request.getChildren("privilages").get(counterPrivilages).getChildren("actions").size(); counterActions++) {
                actionsObject.add(request.getChildren("privilages").get(counterPrivilages).getChildren("actions").get(counterActions).strValue());

            }
            privilageObj.put("actions", actionsObject);
            privilages.add(privilageObj);
        }
        createRoleObj.put("privileges", privilages);

        ArrayList<DBObject> rolesObject = new ArrayList<>();
        for (int counterRoles = 0; counterRoles < request.getChildren("roles").size(); counterRoles++) {
            DBObject roleObj = new BasicDBObject();
            roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
            roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
            rolesObject.add(roleObj);
        }

        createRoleObj.put("roles", rolesObject);
        System.out.println(createRoleObj.toString());
        Document a = db.runCommand((Bson) createRoleObj);

        System.out.println(a.toJson().toString());
        return v;

    }

    @RequestResponse
    public Value updateRole(Value request) {

        Value v = Value.create();
        DBObject createRoleObj = new BasicDBObject();
        createRoleObj.put("updateRole", request.getFirstChild("roleName").strValue());
        ArrayList<DBObject> privilages = new ArrayList();
        for (int counterPrivilages = 0; counterPrivilages < request.getChildren("privilages").size(); counterPrivilages++) {
            DBObject privilageObj = new BasicDBObject();
            DBObject resourceObj = new BasicDBObject();
            Map<String, ValueVector> resourceMap = request.getChildren("privilages").get(counterPrivilages).getFirstChild("resource").children();
            Set<String> keyResource = resourceMap.keySet();
            Iterator<String> keyResourceIterator = keyResource.iterator();
            while (keyResourceIterator.hasNext()) {
                String resourceName = keyResourceIterator.next();

                ValueVector resourceData = resourceMap.get(resourceName);
                if (resourceData.get(0).isBool()) {
                    resourceObj.put(resourceName, resourceData.get(0).boolValue());
                }
                if (resourceData.get(0).isString()) {
                    resourceObj.put(resourceName, resourceData.get(0).strValue());
                }
            }

            privilageObj.put("resource", resourceObj);
            ArrayList<String> actionsObject = new ArrayList();
            for (int counterActions = 0; counterActions < request.getChildren("privilages").get(counterPrivilages).getChildren("actions").size(); counterActions++) {
                actionsObject.add(request.getChildren("privilages").get(counterPrivilages).getChildren("actions").get(counterActions).strValue());

            }
            privilageObj.put("actions", actionsObject);
            privilages.add(privilageObj);
        }
        createRoleObj.put("privileges", privilages);
        ArrayList<DBObject> rolesObject = new ArrayList<>();
        for (int counterRoles = 0; counterRoles < request.getChildren("roles").size(); counterRoles++) {
            DBObject roleObj = new BasicDBObject();
            roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
            roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
            rolesObject.add(roleObj);
        }

        createRoleObj.put("roles", rolesObject);
        System.out.println(createRoleObj.toString());
        Document response = db.runCommand((Bson) createRoleObj);

        System.out.println(response.toJson().toString());
        return v;

    }
    
    @RequestResponse
    public Value dropRole(Value request) {
        
        Value v = Value.create();
        DBObject createRoleObj = new BasicDBObject();
        createRoleObj.put("dropRole", request.getFirstChild("roleName").strValue());
        
        System.out.println(createRoleObj.toString());
        Document response = db.runCommand((Bson) createRoleObj);

        System.out.println(response.toJson().toString());
        return v;

    }

    @RequestResponse
    public Value readRoles(Value request) {
        Value v = Value.create();
        MongoCollection<BsonDocument> collection = db.getCollection("system.roles", BsonDocument.class);
        FindIterable<BsonDocument> iteratable = collection.find();
        iteratable.forEach(new Block<BsonDocument>() {
            @Override
            public void apply(BsonDocument t) {
                Value queryValue = processQueryRow(t);

                v.getChildren("roles").add(queryValue);
            }
        });

        return v;
    }
    @RequestResponse
    public Value createUser (Value request){
      Value v = Value.create();
        DBObject createUserObj = new BasicDBObject();
        createUserObj.put("createUser", request.getFirstChild("username").strValue());
        ArrayList<BasicDBObject> rolesObj = new ArrayList();
        for (int counterRoles =0 ; counterRoles < request.getChildren("roles").size(); counterRoles++){
           BasicDBObject roleObj = new BasicDBObject();
           roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
           if (request.getChildren("roles").get(counterRoles).hasChildren("db")){
             roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
           };
           rolesObj.add(roleObj);
        }  
        createUserObj.put("roles", rolesObj);
        createUserObj.put("roles",request.getFirstChild("password").strValue());
        Document response = db.runCommand((Bson) createUserObj);
      return v;          
    }
    
    @RequestResponse
    public Value updateUser (Value request){
      Value v = Value.create();
        DBObject updateUserObj = new BasicDBObject();
        updateUserObj.put("updateUser", request.getFirstChild("username").strValue());
        ArrayList<BasicDBObject> rolesObj = new ArrayList();
        for (int counterRoles =0 ; counterRoles < request.getChildren("roles").size(); counterRoles++){
           BasicDBObject roleObj = new BasicDBObject();
           roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
           if (request.getChildren("roles").get(counterRoles).hasChildren("db")){
             roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
           };
           rolesObj.add(roleObj);
        }  
        updateUserObj.put("roles", rolesObj);
        updateUserObj.put("roles",request.getFirstChild("password").strValue());
        Document response = db.runCommand((Bson) updateUserObj);
      return v;          
    }
    
    
    @RequestResponse
    public Value getDBReadConcern() {
        ReadConcern readConcern = db.getReadConcern();
        return (processQueryRow(readConcern.asDocument()));
    }

    @RequestResponse
    public Value aggregate(Value request) throws FaultException {
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

    private BsonDocument prepareBsonQueryData(BsonDocument bsonQueryDocument, Value request) throws FaultException {
        Set<String> keySet = bsonQueryDocument.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();
        while (iteratorKeySet.hasNext()) {
            String keyName = iteratorKeySet.next();

            if (bsonQueryDocument.isString(keyName)) {

                BsonString conditionValue = bsonQueryDocument.getString(keyName);

                if (conditionValue.getValue().startsWith("$")) {
                    String conditionValueName = conditionValue.getValue().substring(1);

                    if (request.getFirstChild(conditionValueName).isInt()) {

                        if (is64) {
                            BsonInt64 objToInsert = new BsonInt64(request.getFirstChild(conditionValueName).intValue());
                            bsonQueryDocument.put(keyName, objToInsert);
                        } else {
                            BsonInt32 objToInsert = new BsonInt32(request.getFirstChild(conditionValueName).intValue());
                            bsonQueryDocument.put(keyName, objToInsert);
                        }
                    }
                    if (request.getFirstChild(conditionValueName).isDouble()) {

                        BsonDouble objToInsert = new BsonDouble(request.getFirstChild(conditionValueName).doubleValue());
                        bsonQueryDocument.put(keyName, objToInsert);
                    }
                    if (request.getFirstChild(conditionValueName).isString()) {

                        BsonString objToInsert = new BsonString(request.getFirstChild(conditionValueName).strValue());
                        bsonQueryDocument.put(keyName, objToInsert);

                    }
                    if (request.getFirstChild(conditionValueName).hasChildren()) {

                        if (request.getFirstChild(conditionValueName).hasChildren("@type")) {

                            if (request.getFirstChild(conditionValueName).getFirstChild("@type").strValue().equals("Date")) {

                                BsonDateTime objToInsert = new BsonDateTime(request.getFirstChild(conditionValueName).longValue());

                                bsonQueryDocument.put(keyName, objToInsert);

                            }
                            if (request.getFirstChild(conditionValueName).getFirstChild("@type").strValue().equals("Point")) {

                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("Point"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                coordinates.add(new BsonDouble(request.getFirstChild(conditionValueName).getFirstChild("coordinates").getFirstChild("lat").doubleValue()));
                                coordinates.add(new BsonDouble(request.getFirstChild(conditionValueName).getFirstChild("coordinates").getFirstChild("log").doubleValue()));
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);

                                bsonQueryDocument.put(keyName, bsonObj);

                            }
                            if (request.getFirstChild(conditionValueName).getFirstChild("@type").strValue().equals("Point")) {

                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("Point"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                coordinates.add(new BsonDouble(request.getFirstChild(conditionValueName).getFirstChild("coordinates").getFirstChild("lat").doubleValue()));
                                coordinates.add(new BsonDouble(request.getFirstChild(conditionValueName).getFirstChild("coordinates").getFirstChild("log").doubleValue()));
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);

                                bsonQueryDocument.put(keyName, bsonObj);

                            }
                        } else {

                            bsonQueryDocument.put(keyName, createDocument(request.getFirstChild(conditionValueName)));
                        }
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
                if (!bsonQueryDocument.getDocument(keyName).containsKey("type")) {

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
                                        addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).isInt() if object array " + conditionValue);
                                        if (is64) {
                                            supportListValueCondition.add(new BsonInt64(request.getFirstChild(conditionValue).intValue()));
                                        } else {
                                            supportListValueCondition.add(new BsonInt32(request.getFirstChild(conditionValue).intValue()));
                                        }
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
                                    addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).isInt() " + conditionValue);
                                    if (is64) {
                                        conditionObject.append(conditionName, new BsonInt64(request.getFirstChild(conditionValue).intValue()));
                                    } else {
                                        conditionObject.append(conditionName, new BsonInt32(request.getFirstChild(conditionValue).intValue()));
                                    }
                                }
                                if (request.getFirstChild(conditionValue).isDouble()) {
                                    addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).isDouble() " + conditionValue);
                                    conditionObject.put(conditionName, new BsonDouble(request.getFirstChild(conditionValue).doubleValue()));

                                }
                                if (request.getFirstChild(conditionValue).isString()) {
                                    addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).isString() " + conditionValue);
                                    conditionObject.put(conditionName, new BsonString(request.getFirstChild(conditionValue).strValue()));
                                }

                                if (request.getFirstChild(conditionValue).hasChildren()) {
                                    addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).hasChildren() " + conditionValue);
                                    if (request.getFirstChild(conditionValue).hasChildren("@type")) {

                                        if (request.getFirstChild(conditionValue).getFirstChild("@type").strValue().equals("Date")) {

                                            BsonDateTime objToInsert = new BsonDateTime(request.getFirstChild(conditionValue).longValue());

                                            conditionObject.put(conditionName, objToInsert);
                                        }

                                        if (request.getFirstChild(conditionValue).getFirstChild("@type").strValue().equals("Point")) {

                                            ArrayList<BsonElement> bsonPoint = new ArrayList();
                                            BsonElement typeElement = new BsonElement("type", new BsonString("Point"));
                                            bsonPoint.add(typeElement);
                                            BsonArray coordinates = new BsonArray();
                                            coordinates.add(new BsonDouble(request.getFirstChild(conditionValue).getFirstChild("coordinates").getFirstChild("lat").doubleValue()));
                                            coordinates.add(new BsonDouble(request.getFirstChild(conditionValue).getFirstChild("coordinates").getFirstChild("log").doubleValue()));
                                            BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                            bsonPoint.add(coordinateElement);
                                            BsonDocument bsonObj = new BsonDocument(bsonPoint);
                                            bsonQueryDocument.put(keyName, bsonObj);

                                        }
                                    } else {
                                        addLog("prepareBsonQueryData>>> request.getFirstChild(conditionValue).hasChildren() not Date ");
                                        conditionObject.put(conditionName, createDocument(request.getFirstChild(conditionValue)));
                                    }
                                }

                            }
                            if (conditionObject.get(conditionName).isDocument()) {

                                addLog("prepareBsonQueryData>>> conditionObject.get(conditionName).isDocument() " + conditionName);
                                prepareBsonQueryData(conditionObject.get(conditionName).asDocument(), request);

                            }
                        }
                    }

                    bsonQueryDocument.put(keyName, conditionObject);
                }
            }
        }
        return bsonQueryDocument;

    }

    private BsonDocument createDocument(Value request) throws FaultException {
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
                        if (valueVector.get(counterValueVector).hasChildren("@type")) {
                            if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("Date")) {
                                DateTimeZone.setDefault(zone);
                                BsonDateTime bsonObj = new BsonDateTime(valueVector.get(counterValueVector).longValue());
                                if (valueVector.size() == 1) {
                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }

                            } else if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("Point")) {
                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("Point"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                coordinates.add(new BsonDouble(valueVector.get(counterValueVector).getFirstChild("coordinates").getFirstChild("lat").doubleValue()));
                                coordinates.add(new BsonDouble(valueVector.get(counterValueVector).getFirstChild("coordinates").getFirstChild("log").doubleValue()));
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);
                                if (valueVector.size() == 1) {
                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }
                            } else if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("LineString")) {
                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("LineString"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                for (int counter = 0; counter < valueVector.get(counterValueVector).getChildren("coordinates").size(); counter++) {
                                    BsonArray coord = new BsonArray();
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("lat").doubleValue()));
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("log").doubleValue()));
                                    coordinates.add(coord);
                                }
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);
                                if (valueVector.size() == 1) {
                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }
                            } else if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("LineString")) {
                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("LineString"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                for (int counter = 0; counter < valueVector.get(counterValueVector).getChildren("coordinates").size(); counter++) {
                                    BsonArray coord = new BsonArray();
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("lat").doubleValue()));
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("log").doubleValue()));
                                    coordinates.add(coord);
                                }
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);
                                if (valueVector.size() == 1) {
                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }
                            } else if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("Polygon")) {
                                ArrayList<BsonElement> bsonPoint = new ArrayList();
                                BsonElement typeElement = new BsonElement("type", new BsonString("Polygon"));
                                bsonPoint.add(typeElement);
                                BsonArray coordinates = new BsonArray();
                                for (int counter = 0; counter < valueVector.get(counterValueVector).getChildren("coordinates").size(); counter++) {
                                    BsonArray coord = new BsonArray();
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("lat").doubleValue()));
                                    coord.add(new BsonDouble(valueVector.get(counterValueVector).getChildren("coordinates").get(counter).getFirstChild("log").doubleValue()));
                                    coordinates.add(coord);
                                }
                                BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
                                bsonPoint.add(coordinateElement);
                                BsonDocument bsonObj = new BsonDocument(bsonPoint);
                                if (valueVector.size() == 1) {
                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }
                            } else {
                                throw new FaultException("ComplexTypeNotSupported");
                            }

                        } else {
                            bsonArray.add(counterValueVector, createDocument(valueVector.get(counterValueVector)));
                        }
                    }

                    if (valueVector.get(counterValueVector).isInt()) {
                        addLog("createDocument>>> valueVector.get(counterValueVector).isInt() ");
                        if (is64) {
                            BsonInt64 bsonObj = new BsonInt64(valueVector.get(counterValueVector).intValue());
                            if (valueVector.size() == 1) {
                                bsonDocument.append(entry.getKey(), bsonObj);
                            } else {
                                bsonArray.add(counterValueVector, bsonObj);
                            }

                        } else {
                            BsonInt32 bsonObj = new BsonInt32(valueVector.get(counterValueVector).intValue());
                            if (valueVector.size() == 1) {
                                bsonDocument.append(entry.getKey(), bsonObj);
                            } else {
                                bsonArray.add(counterValueVector, bsonObj);
                            }
                        }

                    }
                    if (valueVector.get(counterValueVector).isDouble()) {
                        addLog("createDocument>>> valueVector.get(counterValueVector).isDouble() ");
                        BsonDouble bsonObj = new BsonDouble(valueVector.get(counterValueVector).doubleValue());
                        if (valueVector.size() == 1) {
                            bsonDocument.append(entry.getKey(), bsonObj);
                        } else {
                            bsonArray.add(counterValueVector, bsonObj);
                        }
                    }
                    if (valueVector.get(counterValueVector).isString()) {
                        addLog("createDocument>>> valueVector.get(counterValueVector).isString() ");
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
            } else if (document.isInt64(nameField)) {
                v.getChildren(nameField).add(Value.create(document.getInt64(nameField).getValue()));
            } else if (document.isDouble(nameField)) {
                v.getChildren(nameField).add(Value.create(document.getDouble(nameField).getValue()));
            } else if (document.isDateTime(nameField)) {
                Date date = new Date(document.getDateTime(nameField).getValue());
                v.getChildren(nameField).add(Value.create(date.getTime()));
                v.getFirstChild(nameField).getNewChild("@type").add(Value.create("Date"));
                v.getFirstChild(nameField).getFirstChild("@type").getNewChild("DateStr").add(Value.create(date.toString()));
            } else if (document.isDocument(nameField)) {
                if ((document.getDocument(nameField).containsKey("type"))
                        && (document.getDocument(nameField).containsKey("coordinates"))) {
                    if (document.getDocument(nameField).containsValue(new BsonString("Point"))) {
                        v.getFirstChild(nameField).getFirstChild("@type").add(Value.create("Point"));
                        BsonArray coordinates = document.getDocument(nameField).getArray("coordinates");
                        v.getFirstChild(nameField).getFirstChild("coordinates").getFirstChild("lat").add(Value.create(coordinates.get(0).asDouble().getValue()));
                        v.getFirstChild(nameField).getFirstChild("coordinates").getFirstChild("log").add(Value.create(coordinates.get(1).asDouble().getValue()));
                    }
                    if (document.getDocument(nameField).containsValue(new BsonString("LineString"))) {

                        v.getFirstChild(nameField).getFirstChild("@type").add(Value.create("LineString"));
                        BsonArray coordinates = document.getDocument(nameField).getArray("coordinates");
                        for (int counter = 0; counter < coordinates.size(); counter++) {
                            BsonArray cood = coordinates.get(counter).asArray();
                            v.getFirstChild(nameField).getChildren("coordinates").get(counter).getFirstChild("lat").add(Value.create(cood.get(0).asDouble().getValue()));
                            v.getFirstChild(nameField).getChildren("coordinates").get(counter).getFirstChild("log").add(Value.create(cood.get(1).asDouble().getValue()));
                        }
                    }
                    if (document.getDocument(nameField).containsValue(new BsonString("Polygon"))) {

                        v.getFirstChild(nameField).getFirstChild("@type").add(Value.create("Polygon"));
                        BsonArray coordinates = document.getDocument(nameField).getArray("coordinates");
                        for (int counter = 0; counter < coordinates.size(); counter++) {
                            BsonArray cood = coordinates.get(counter).asArray();
                            v.getFirstChild(nameField).getChildren("coordinates").get(counter).getFirstChild("lat").add(Value.create(cood.get(0).asDouble().getValue()));
                            v.getFirstChild(nameField).getChildren("coordinates").get(counter).getFirstChild("log").add(Value.create(cood.get(1).asDouble().getValue()));
                        }
                    }
                } else {
                    v.getChildren(nameField).add(processQueryRow(document.getDocument(nameField)));
                }
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
                    } else if (obj instanceof BsonInt64) {
                        BsonInt64 bsonObj = (BsonInt64) obj;
                        v.getChildren(nameField).add(Value.create(bsonObj.getValue()));
                    } else if (obj instanceof BsonDouble) {
                        BsonDouble bsonObj = (BsonDouble) obj;
                        v.getChildren(nameField).add(Value.create(bsonObj.getValue()));
                    } else if (obj instanceof BsonDateTime) {
                        BsonDateTime date = BsonDateTime.class.cast(obj);
                        Value objValue = Value.create(date.getValue());
                        objValue.getNewChild("@type").add(Value.create("Date"));
                        objValue.getFirstChild("@type").getNewChild("DateStr").add(Value.create(date.toString()));
                        v.getChildren(nameField).add(objValue);
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

    private void printlnJson(String level, BsonDocument bsonDocument) {
        if (jsonDebuger) {
            System.out.println(level + " " + bsonDocument.toJson());
        }
    }

    private void addLog(String msg) {
        if (logStream) {
            logString += "\r\n" + msg;
        }
    }

    private void showLog() {

        logString = "Processing Steps at " + System.currentTimeMillis();

    }
}
