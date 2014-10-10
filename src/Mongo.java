import com.mongodb.*;

public class Mongo {
    private MongoClient _client = null;
    private DB _db = null;

    public Mongo(MongoClient client, DB db){
        this._client = client;
        this._db = db;
    }

    public WriteResult createDocument(){
        BasicDBObject newDoc = new BasicDBObject("name", "Conor Hayes");
        newDoc.append("T-Number","t00148752");
        newDoc.append("Year", 4);
        newDoc.append("modules", new BasicDBObject("Peter Given", "Advanced Database Programming")
                .append("Owen Mackessy", "Software Design Patterns"));
        DBCollection students = _db.getCollection("students");
        return students.insert(newDoc, WriteConcern.NONE);
    }

    public DBObject retrieveDocument(){
        return _db.getCollection("students").findOne();
    }

    public WriteResult updateDocument(){
        BasicDBObject newDocument = new BasicDBObject();
        newDocument.append("$set", new BasicDBObject().append("Hometown", "Tralee"));
        BasicDBObject searchQuery = new BasicDBObject().append("name", "Conor Hayes");
        return _db.getCollection("students").update(searchQuery, newDocument);
    }

    public WriteResult deleteDocument(){
        DBObject student = _db.getCollection("students").findOne();
        return _db.getCollection("students").remove(student);
    }

    public void runMapReduce(DBCollection bands){
        MapReduceOutput out = bands.mapReduce(new MapReduceCommand(bands,
                "function(){ " +
                    "for (var album in this.albums) { " +
                        "emit({band: this.name}, 1); " +
                    "} " +
                "}",
                "function(key, values){ " +
                    "var sum = 0; " +
                    "for (var i in values) { " +
                        "sum += values[i]; " +
                    "} " +
                "return sum; }",
                null, MapReduceCommand.OutputType.INLINE, null));
        System.out.println("Mapreduce results");
        for (DBObject o : out.results()) {
            System.out.println(o.toString());
        }
    }
}
