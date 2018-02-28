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
import com.mongodb.ReadConcernLevel;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;


import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jolie.runtime.ByteArray;
import jolie.runtime.FaultException;
import jolie.runtime.ValueVector;
import jolie.runtime.typing.TypeCastingException;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonArray;
import org.bson.BsonBinary;
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
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author maschio
 */
@CanUseJars({
    "mongo-java-driver-3.6.3.jar",
    "mongodb-driver-3.6.3.jar",
    "mongodb-driver-core-3.6.3.jar",
    "joda-time-2.4.jar",
    "commons-codec-1.10.jar"
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
    private static DateTimeZone zone;
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

            zone = DateTimeZone.forID(timeZone);

            ServerAddress serverAddress = new ServerAddress(host, port);
            MongoCredential credential = MongoCredential.createCredential(username, dbname, password.toCharArray());
            mongoClientOptions = MongoClientOptions.builder().build();
            
            
            if (null != mongoClient) {
                System.out.println("recovering client");
                db = mongoClient.getDatabase(dbname);
            } else {
                
                mongoClient = new MongoClient(serverAddress, credential, mongoClientOptions);
                db = mongoClient.getDatabase(dbname);
            }

        } catch (MongoException ex) {
            throw new FaultException("LoginConnection", ex);
        }
    }

    @RequestResponse
    void close(Value request) {
        mongoClient.close();
    }

    @RequestResponse
    public Value find(Value request) throws FaultException {
        Value v = Value.create();
        FindIterable<BsonDocument> iterable = null;
        ;
        try {

            String collectionName = request.getFirstChild("collection").strValue();
            MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
            if (request.hasChildren("readConcern")) {
                
                ReadConcern readConcern = new ReadConcern(ReadConcernLevel.fromString(request.getFirstChild("readConcern").strValue()));
                collection.withReadConcern(readConcern);
            }

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

                if (request.hasChildren("sort") && request.hasChildren("limit") && request.hasChildren("skip")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    int limitQuery = request.getFirstChild("limit").intValue();
                    int skipPosition = request.getFirstChild("skip").intValue();
                    iterable = collection.find(bsonQueryDocument).sort(bsonSortDocument).limit(limitQuery).skip(skipPosition);
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
                    int findLimit = request.getFirstChild("limit").intValue();
                   
                    iterable = collection.find(new Document()).sort(bsonSortDocument);  ///.sort(bsonSortDocument).limit(limitQuery);
                }
                if (request.hasChildren("sort") && !request.hasChildren("limit")) {
                    BsonDocument bsonSortDocument = BsonDocument.parse(request.getFirstChild("sort").strValue());
                    prepareBsonQueryData(bsonSortDocument, request.getFirstChild("sort"));
                    printlnJson("Query sort", bsonSortDocument);
                    iterable = collection.find(new Document()).sort(bsonSortDocument);
                }
                if (!request.hasChildren("sort") && request.hasChildren("limit")) {
                    int limitQuery = request.getFirstChild("limit").intValue();
                    iterable = collection.find(new Document()).limit(limitQuery);
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
    public Value insert(Value request) throws FaultException {
        try {
            Value v = Value.create();
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonDocument = createDocument(request.getFirstChild("document"));
            printlnJson("insert document", bsonDocument);

            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }
            if (request.hasChildren("options")) {
                InsertOneOptions insertOneOptions = new InsertOneOptions();
                insertOneOptions.bypassDocumentValidation(request.getFirstChild("options").getFirstChild("bypassDocumentValidation").boolValue());
                db.getCollection(collectionName, BsonDocument.class).insertOne(bsonDocument, insertOneOptions);
            } else {
                db.getCollection(collectionName, BsonDocument.class).insertOne(bsonDocument);

            }

            bsonDocument.get("_id").asObjectId().getValue().toByteArray();

            String str = new String(bsonDocument.get("_id").asObjectId().getValue().toHexString());
            Value objValue = Value.create(str);
            v.getNewChild("_id").assignValue(objValue);
            v.getFirstChild("_id").getNewChild("@type").assignValue(Value.create("ObjectId"));

            return v;
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        }

    }

    @RequestResponse
    public Value insertMany(Value request) throws FaultException {
        Value v = Value.create();
        String collectionName = request.getFirstChild("collection").strValue();
        List<BsonDocument> documents = new ArrayList();
        BsonDocument bsonDocument;
        try {

            for (int counterDocuments = 0; counterDocuments < request.getChildren("document").size(); counterDocuments++) {
                bsonDocument = createDocument(request.getChildren("document").get(counterDocuments));
                documents.add(bsonDocument);
            }
            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }
            if (request.hasChildren("options")) {
                InsertManyOptions insertManyOptions = new InsertManyOptions();
                insertManyOptions.ordered(request.getFirstChild("options").getFirstChild("ordered").boolValue());
                insertManyOptions.ordered(request.getFirstChild("options").getFirstChild("ordered").boolValue());
                insertManyOptions.bypassDocumentValidation(request.getFirstChild("options").getFirstChild("bypassDocumentValidation").boolValue());
                db.getCollection(collectionName, BsonDocument.class).insertMany(documents, insertManyOptions);
            } else {
                db.getCollection(collectionName, BsonDocument.class).insertMany(documents);

            };
            db.getCollection(collectionName, BsonDocument.class).insertMany(documents);
            for (int counterDocuments = 0; counterDocuments < request.getChildren("document").size(); counterDocuments++) {
                String str = new String(Hex.decodeHex(documents.get(counterDocuments).get("_id").asObjectId().getValue().toHexString().toCharArray()), StandardCharsets.UTF_8);
                Value result = Value.create();
                result.getNewChild("_id").assignValue(Value.create(str));
                result.getFirstChild("_id").getNewChild("@type").assignValue(Value.create("ObjectID"));
                v.getChildren("results").add(result);
            }

            
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (DecoderException ex) {
            Logger.getLogger(MongoDbConnector.class.getName()).log(Level.SEVERE, null, ex);
        } 
         return v;
    }

    @RequestResponse
    public Value delete(Value request) throws FaultException {
        try {
            Value v = Value.create();
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Delete filter", bsonQueryDocument);
            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }

            DeleteResult resultDelete = db.getCollection(collectionName, BsonDocument.class).deleteOne(bsonQueryDocument);
            v.getNewChild("deleteCount").add(Value.create(resultDelete.getDeletedCount()));
            return v;
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }
    }

    @RequestResponse
    public Value deleteMany(Value request) throws FaultException {
        try {
            Value v = Value.create();
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Delete filter", bsonQueryDocument);
            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }
            DeleteResult resultDelete = db.getCollection(collectionName, BsonDocument.class).deleteMany(bsonQueryDocument);

            v.getNewChild("deletedCount").add(Value.create(resultDelete.getDeletedCount()));
            return v;
        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }
    }

    @RequestResponse

    public Value updateMany(Value request) throws FaultException {
        try {
            String collectionName = request.getFirstChild("collection").strValue();
            Value v = Value.create();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Update filter", bsonQueryDocument);
            BsonDocument bsonDocument = BsonDocument.parse(request.getFirstChild("documentUpdate").strValue());
            printlnJson("Update documentUpdate", bsonDocument);
            prepareBsonQueryData(bsonDocument, request.getFirstChild("documentUpdate"));
            printlnJson("Update documentUpdate", bsonDocument);
            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }
            if (request.hasChildren("options")) {
                UpdateOptions updateOptions = new UpdateOptions();
                updateOptions.upsert(request.getFirstChild("options").getFirstChild("upsert").boolValue());
                updateOptions.bypassDocumentValidation(request.getFirstChild("options").getFirstChild("bypassDocumentValidation").boolValue());
                UpdateResult resultUpdate = db.getCollection(collectionName, BsonDocument.class).updateMany(bsonQueryDocument, bsonDocument, updateOptions);
                v.getNewChild("matchedCount").assignValue(Value.create(resultUpdate.getMatchedCount()));
                v.getNewChild("modifiedCount").assignValue(Value.create(resultUpdate.getModifiedCount()));

            } else {
                UpdateResult resultUpdate = db.getCollection(collectionName, BsonDocument.class).updateMany(bsonQueryDocument, bsonDocument);
                v.getNewChild("matchedCount").assignValue(Value.create(resultUpdate.getMatchedCount()));
                v.getNewChild("modifiedCount").assignValue(Value.create(resultUpdate.getModifiedCount()));
            }

            return v;

        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
        } catch (JsonParseException ex) {
            throw new FaultException("JsonParseException", ex);
        }

    }

    @RequestResponse
    public Value update(Value request) throws FaultException {
        try {
            Value v = Value.create();
            String collectionName = request.getFirstChild("collection").strValue();
            BsonDocument bsonQueryDocument = BsonDocument.parse(request.getFirstChild("filter").strValue());
            prepareBsonQueryData(bsonQueryDocument, request.getFirstChild("filter"));
            printlnJson("Update filter", bsonQueryDocument);
            BsonDocument bsonDocument = BsonDocument.parse(request.getFirstChild("documentUpdate").strValue());
            printlnJson("Update documentUpdate", bsonDocument);
            prepareBsonQueryData(bsonDocument, request.getFirstChild("documentUpdate"));
            printlnJson("Update documentUpdate", bsonDocument);
            showLog();

            if (request.hasChildren("writeConcern")) {
                WriteConcern writeConcern = new WriteConcern();
                if (request.getFirstChild("writeConcern").hasChildren("journal")) {
                    writeConcern.withJournal(request.getFirstChild("writeConcern").getFirstChild("journal").boolValue());
                }
                if (request.getFirstChild("writeConcern").hasChildren("w")) {
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isInt()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").intValue());
                    }
                    if (request.getFirstChild("writeConcern").getFirstChild("w").isString()) {
                        writeConcern.withW(request.getFirstChild("writeConcern").getFirstChild("w").strValue());
                    }
                }
                if (request.getFirstChild("writeConcern").hasChildren("timeout")) {
                    writeConcern.withWTimeout(request.getFirstChild("writeConcern").getFirstChild("timeout").longValue(), TimeUnit.MILLISECONDS);
                }

                db.getCollection(collectionName, BsonDocument.class).withWriteConcern(writeConcern);
            }
            if (request.hasChildren("options")) {
                UpdateOptions updateOptions = new UpdateOptions();
                updateOptions.upsert(request.getFirstChild("options").getFirstChild("upsert").boolValue());
                updateOptions.bypassDocumentValidation(request.getFirstChild("options").getFirstChild("bypassDocumentValidation").boolValue());
                UpdateResult resultUpdate = db.getCollection(collectionName, BsonDocument.class).updateOne(bsonQueryDocument, bsonDocument, updateOptions);
                v.getNewChild("matchedCount").assignValue(Value.create(resultUpdate.getMatchedCount()));
                v.getNewChild("modifiedCount").assignValue(Value.create(resultUpdate.getModifiedCount()));

            } else {
                UpdateResult resultUpdate = db.getCollection(collectionName, BsonDocument.class).updateOne(bsonQueryDocument, bsonDocument);
                v.getNewChild("matchedCount").assignValue(Value.create(resultUpdate.getMatchedCount()));
                v.getNewChild("modifiedCount").assignValue(Value.create(resultUpdate.getModifiedCount()));
            }
            return v;

        } catch (MongoException ex) {
            throw new FaultException("MongoException", ex);
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
            counterDatabase++;
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

 /*   @RequestResponse
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
        
    }*/
    @RequestResponse
    public Value createUser(Value request) {
        Value v = Value.create();
        DBObject createUserObj = new BasicDBObject();
        createUserObj.put("createUser", request.getFirstChild("username").strValue());
        ArrayList<BasicDBObject> rolesObj = new ArrayList();
        for (int counterRoles = 0; counterRoles < request.getChildren("roles").size(); counterRoles++) {
            BasicDBObject roleObj = new BasicDBObject();
            roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
            if (request.getChildren("roles").get(counterRoles).hasChildren("db")) {
                roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
            };
            rolesObj.add(roleObj);
        }
        createUserObj.put("roles", rolesObj);
        createUserObj.put("roles", request.getFirstChild("password").strValue());
        Document response = db.runCommand((Bson) createUserObj);
        return v;
    }

    @RequestResponse
    public Value updateUser(Value request) {
        Value v = Value.create();
        DBObject updateUserObj = new BasicDBObject();
        updateUserObj.put("updateUser", request.getFirstChild("username").strValue());
        ArrayList<BasicDBObject> rolesObj = new ArrayList();
        for (int counterRoles = 0; counterRoles < request.getChildren("roles").size(); counterRoles++) {
            BasicDBObject roleObj = new BasicDBObject();
            roleObj.put("role", request.getChildren("roles").get(counterRoles).getFirstChild("role").strValue());
            if (request.getChildren("roles").get(counterRoles).hasChildren("db")) {
                roleObj.put("db", request.getChildren("roles").get(counterRoles).getFirstChild("db").strValue());
            };
            rolesObj.add(roleObj);
        }
        updateUserObj.put("roles", rolesObj);
        updateUserObj.put("roles", request.getFirstChild("password").strValue());
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
        MongoCollection<BsonDocument> collection = db.getCollection(collectionName, BsonDocument.class);
        if (request.hasChildren("readConcern")) {
            ReadConcern readConcern = new ReadConcern(ReadConcernLevel.fromString(request.getFirstChild("readConcern").strValue()));
            collection.withReadConcern(readConcern);
        }
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
                Value queryResult = processQueryRow(t);
                v.getChildren("document").add(queryResult);
            }
        });

        return v;
    }
    private BsonValue convertComplexType (Value v){
       BsonValue bsonValue = null;
       if (v.getFirstChild("@type").strValue().equals("ObjectId")) {
             ObjectId objId = new ObjectId(v.strValue());
             bsonValue = new BsonObjectId(objId);
       }
      if (v.getFirstChild("@type").strValue().equals("Date")) {
           bsonValue  = new BsonDateTime(v.longValue());
      }
      if (v.getFirstChild("@type").strValue().equals("Point")) {
              ArrayList<BsonElement> bsonPoint = new ArrayList();
              BsonElement typeElement = new BsonElement("type", new BsonString("Point"));
              bsonPoint.add(typeElement);
              BsonArray coordinates = new BsonArray();
              coordinates.add(new BsonDouble(v.getFirstChild("coordinates").getFirstChild("lat").doubleValue()));
              coordinates.add(new BsonDouble(v.getFirstChild("coordinates").getFirstChild("log").doubleValue()));
              BsonElement coordinateElement = new BsonElement("coordinates", coordinates);
              bsonPoint.add(coordinateElement);
              bsonValue = new BsonDocument(bsonPoint);
           }
                               
       return bsonValue;
    }
    private BsonValue convertNaturalTypeToBson (Value v){
        BsonValue bsonValue = null;
        if (v.isInt()){
         if (is64) {
                  bsonValue = new BsonInt64(v.intValue());                
             } else {
                 bsonValue = new BsonInt32(v.intValue());
             }
        }
        if (v.isDouble()){
          bsonValue = new BsonDouble(v.doubleValue());
        }
        
        if (v.isString()){
          bsonValue = new BsonString(v.strValue());
        }
        
        if (v.isLong()){
          bsonValue = new BsonInt64(v.longValue());
        }
        
        
        return bsonValue; 
    }

    private BsonDocument prepareBsonQueryData(BsonDocument bsonQueryDocument, Value request) throws FaultException {
        Set<String> keySet = bsonQueryDocument.keySet();
        Iterator<String> iteratorKeySet = keySet.iterator();

        while (iteratorKeySet.hasNext()) {
            String keyName = iteratorKeySet.next();

            if (bsonQueryDocument.isString(keyName)) {
                BsonString conditionValueName = bsonQueryDocument.getString(keyName);

                if (conditionValueName.getValue().startsWith("$")) {
                    String conditionValueVariableName = conditionValueName.getValue().substring(1);
                //processing single values
                if (request.getChildren(conditionValueVariableName).size() <= 1) {
                    if (request.getFirstChild(conditionValueVariableName).hasChildren("@type")){
                        bsonQueryDocument.put(keyName ,convertComplexType(request.getFirstChild(conditionValueVariableName)));
                    }else if (request.getFirstChild(conditionValueVariableName).hasChildren()){
                       bsonQueryDocument.put(keyName, createDocument(request.getFirstChild(conditionValueVariableName)));
                    }else{
                       bsonQueryDocument.put(keyName, convertNaturalTypeToBson(request.getFirstChild(conditionValueVariableName)));
                    }
                } else{
                    BsonArray condisionArray = new BsonArray();
                        for (int counter = 0; counter < request.getChildren(conditionValueVariableName).size(); counter++) {
                            
                            if (request.getChildren(conditionValueVariableName).get(counter).hasChildren("@type")){
                               condisionArray.add(convertComplexType(request.getChildren(conditionValueVariableName).get(counter)));
                            }else{
                               condisionArray.add(convertNaturalTypeToBson(request.getChildren(conditionValueVariableName).get(counter)));
                            }
                           
                        }
                        bsonQueryDocument.put(keyName, condisionArray);

                    }
                }

            }
  /*          if (bsonQueryDocument.isArray(keyName)) {
                BsonArray array = bsonQueryDocument.getArray(keyName);
                ListIterator<BsonValue> listIterator = array.listIterator();

                while (listIterator.hasNext()) {
                    prepareBsonQueryData(listIterator.next().asDocument(), request);
                }
            }*/

            if (bsonQueryDocument.isDocument(keyName)) {
                  bsonQueryDocument.put(keyName,prepareBsonQueryData(bsonQueryDocument.getDocument(keyName), request));
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

                                DateTime dt = new DateTime(zone.convertUTCToLocal(valueVector.get(counterValueVector).longValue()));
                                BsonDateTime bsonObj = new BsonDateTime(dt.getMillis());

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
                            } else if (valueVector.get(counterValueVector).getFirstChild("@type").strValue().equals("ObjectId")) {
                          
                                ObjectId objId;
                                try {
                                    objId = new ObjectId(valueVector.get(counterValueVector).strValueStrict());
                                    BsonObjectId bsonObj = new BsonObjectId(objId);
                           

                                
                                if (valueVector.size() == 1) {

                                    bsonDocument.append(entry.getKey(), bsonObj);
                                } else {
                                    bsonArray.add(counterValueVector, bsonObj);
                                }     
                                } catch (TypeCastingException ex) {
                                    Logger.getLogger(MongoDbConnector.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            } else {
                                throw new FaultException("ComplexTypeNotSupported");
                            }

                        } else {
                            bsonArray.add(counterValueVector, createDocument(valueVector.get(counterValueVector)));
                        }
                    } else {

                        if (valueVector.get(counterValueVector).isInt()) {
                            // addLog("createDocument>>> valueVector.get(counterValueVector).isInt() ");
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
                        if (valueVector.get(counterValueVector).isByteArray()) {
                            BsonBinary bsonObj = new BsonBinary(valueVector.get(counterValueVector).byteArrayValue().getBytes());
                            if (valueVector.size() == 1) {
                                bsonDocument.append(entry.getKey(), bsonObj);
                            } else {
                                bsonArray.add(counterValueVector, bsonObj);
                            }
                        }
                        if (valueVector.get(counterValueVector).isDouble()) {
                            // addLog("createDocument>>> valueVector.get(counterValueVector).isDouble() ");
                            BsonDouble bsonObj = new BsonDouble(valueVector.get(counterValueVector).doubleValue());
                            if (valueVector.size() == 1) {
                                bsonDocument.append(entry.getKey(), bsonObj);
                            } else {
                                bsonArray.add(counterValueVector, bsonObj);
                            }
                        }
                        if (valueVector.get(counterValueVector).isString()) {
                            // addLog("createDocument>>> valueVector.get(counterValueVector).isString() ");
                            BsonString bsonObj = new BsonString(valueVector.get(counterValueVector).strValue());
                            if (valueVector.size() == 1) {
                                bsonDocument.append(entry.getKey(), bsonObj);
                            } else {
                                bsonArray.add(counterValueVector, bsonObj);
                            }
                        }
                        if (valueVector.get(counterValueVector).isByteArray()) {
                            BsonBinary bsonObj = new BsonBinary(valueVector.get(counterValueVector).byteArrayValue().getBytes());
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

            if (document.isDocument(nameField)) {
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
                v.getChildren(nameField).deepCopy(processArrayQuery(document.getArray(nameField)));
            } else if (document.get(nameField).isObjectId()) {
                BsonObjectId objId;
                objId = document.getObjectId(nameField);
                if (nameField.equals("_id")) {
                 
                        String str;
                        System.out.println("Lenght" + objId.getValue().toByteArray().length);
                        str = objId.getValue().toHexString();
                        v.getNewChild("_id").add(Value.create(str));


                } else {
                        System.out.println("Lenght" + objId.getValue().toByteArray().length);
                        String str = objId.getValue().toHexString();
                        v.getChildren(nameField).add(Value.create(str));
                        v.getFirstChild(nameField).getFirstChild("@type").add(Value.create("ObjectID"));
                 
                }
            } else {
                v.getChildren(nameField).add(ProcessQueryBsonValue(document.get(nameField)));
            }

        }

        return v;

    }

    private ValueVector processArrayQuery(BsonArray array) {
        ValueVector vv = ValueVector.create();
        Iterator<BsonValue> iteratorArray = array.iterator();
        while (iteratorArray.hasNext()) {
            Object obj = iteratorArray.next();
            if (obj instanceof BsonObjectId) {
                BsonObjectId objId = BsonObjectId.class
                        .cast(obj);
                try {
                    String str = new String(Hex.decodeHex(objId.getValue().toHexString().toCharArray()), StandardCharsets.UTF_8);
                    Value objValue = Value.create(str);
                    objValue.getNewChild("@type").add(Value.create("ObjectID"));
                    vv.add(objValue);

                } catch (DecoderException ex) {
                    Logger.getLogger(MongoDbConnector.class.getName()).log(Level.SEVERE, null, ex);
                }

            } else if (obj instanceof BsonDocument) {
                BsonDocument bsonObj = (BsonDocument) obj;
                vv.add(processQueryRow(bsonObj));
            } else if (obj instanceof BsonArray) {
                BsonArray bsonArray = (BsonArray) obj;
                if (bsonArray.getValues().size()==1){
                    vv.add(ProcessQueryBsonValue(bsonArray.get(0)));
                }
            } else {
                vv.add(processQueryRow((BsonDocument) obj));
            }
        }

        return vv;

    }

    private Value ProcessQueryBsonValue(BsonValue bsonValue) {
        Value v = Value.create();
        if (bsonValue instanceof BsonString) {
            BsonString bsonObj = (BsonString) bsonValue;
            v = Value.create(bsonObj.getValue());
        } else if (bsonValue instanceof BsonInt32) {
            BsonInt32 bsonObj = (BsonInt32) bsonValue;
            v = Value.create(bsonObj.getValue());
        } else if (bsonValue instanceof BsonInt64) {
            BsonInt64 bsonObj = (BsonInt64) bsonValue;
            v = Value.create(bsonObj.getValue());
        } else if (bsonValue instanceof BsonDouble) {
            BsonDouble bsonObj = (BsonDouble) bsonValue;
            v = Value.create(bsonObj.getValue());
        } else if (bsonValue instanceof BsonDateTime) {
            BsonDateTime date = BsonDateTime.class.cast(bsonValue);
            v = Value.create(date.getValue());
            v.getNewChild("@type").add(Value.create("Date"));
            v.getFirstChild("@type").getNewChild("DateStr").add(Value.create(date.toString()));
        } else if (bsonValue instanceof BsonObjectId) {
            BsonObjectId objId = BsonObjectId.class
                    .cast(bsonValue);
          
                System.out.println(objId.getValue().toByteArray().length);
                String str = new String(objId.getValue().toHexString());
                v = Value.create(str);
                v.getNewChild("@type").add(Value.create("ObjectID"));

      

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
