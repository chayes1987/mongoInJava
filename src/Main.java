import com.mongodb.*;
import java.net.UnknownHostException;

// ***NOTE: DAEMON NEEDS TO BE RUNNING IN THE BACKGROUND***

public class Main {
    public static void main(String[] args){
        MongoClient client = null;
        DB db;
        Mongo m = null;

        // Create Mongo Client
        try {
            client = new MongoClient( "localhost" , 27017 );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // Create/Get Database
        db = client.getDB( "myTestDB" );

        // Create Mongo Class instance
        if(client != null && db != null)
            m = new Mongo(client, db);
        else
            System.out.println("Client or DB Invalid.");

        // Clear collection
        db.getCollection("students").drop();
        db.getCollection("bands").drop();

        // Create
        if(m.createDocument().getError() == null) {
            System.out.println("Document created.");
            System.out.println(db.getCollection("students").findOne());
        }else{
            System.out.println("Error in creating document.");
        }

        // Read
        if(m.retrieveDocument() != null) {
            System.out.println("Document retrieved.");
            System.out.println(m.retrieveDocument());
        }else {
            System.out.println("Document not found...");
        }

        // Update
        if(m.updateDocument().getError() == null) {
            System.out.println("Document updated.");
            System.out.println(db.getCollection("students").findOne());
        }else{
            System.out.println("Error in updating document.");
        }

        // Delete
        if(m.deleteDocument().getError() == null) {
            System.out.println("Document deleted.");
            System.out.println(db.getCollection("students").findOne());
        }else{
            System.out.println("Error in deleting document.");
        }

        // Populate collection to perform Mapreduce
        DBCollection bands = db.getCollection("bands");
        BasicDBObject band = new BasicDBObject();
        band.put("name", "U2");
        String albums[] = {"The Joshua Tree", "War", "Achtung Baby"};
        band.put("albums", albums);
        bands.insert(band);
        band = new BasicDBObject();
        band.put("name", "The Beatles");
        String albums1[] = {"Abbey Road", "Let It Be"};
        band.put("albums", albums1);
        bands.insert(band);
        // Mapreduce
        m.runMapReduce(bands);
    }
}
