package Query_docs;

import org.bson.types.ObjectId;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Sample {
	public static void main(String ar[]){
		try{
			MongoClient mongoClient = new MongoClient("192.168.2.57",27018);
			DB db = mongoClient.getDB("Dataset_5GB");
			int count = 0;
			DBCollection inv = db.getCollection("Inventory");
			DBCursor cur = inv.find();
			while(cur.hasNext()){
				BasicDBObject val = (BasicDBObject)cur.next();
				BasicDBObject copy = (BasicDBObject) val.copy();
				
				
				Object old_id = val.get("_id");
				ObjectId new_id = new ObjectId();
				inv.update(new BasicDBObject("_id",old_id), new BasicDBObject("$set",new BasicDBObject("_id",new_id)));
								
				count++;
				System.out.println(count);
				
			}
					
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
