package Query_docs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class Query7_embed_loop {
	public static void main(String args[]){
		
		try{
			long startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection store_sales = db.getCollection("partial_result_q2_cur");
			DBCollection item = db.getCollection("item");
			
			//Store all the PK,doc of ca in hashmap for O(1) search
			HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
			DBCursor cur_ss = store_sales.find();

			int count=0;
			
			while(cur_ss.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss.next();
		
				if(map_i.containsKey(val.get("ss_item_sk"))){
					store_sales.update(new BasicDBObject("ss_item_sk",val.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map_i.get(val.get("ss_item_sk")))));					
				}
				else{
					BasicDBObject query_i = new BasicDBObject("i_item_sk",val.get("ss_item_sk"));
					DBCursor cur_i = item.find(query_i);
					while(cur_i.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_i.next();
						doc.removeField("_id");
						doc.removeField("i_item_sk");
						map_i.put(val.get("ss_item_sk"), doc);

						store_sales.update(new BasicDBObject("ss_item_sk",val.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map_i.get(val.get("ss_item_sk")))));				
					}
				
				}
				count++;
				System.out.println(count);
			}
			
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time for partial querying (with embedding): "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		
			
			map_i.clear();
		}
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



