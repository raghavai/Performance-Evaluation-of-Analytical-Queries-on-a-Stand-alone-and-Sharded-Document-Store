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


public class Query46_embed_loop1 {
	public static void main(String args[]){
		
		try{
			long startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection store_sales = db.getCollection("partial_result_q23_agg");
			DBCollection address = db.getCollection("customer_address");
			
			//Store all the PK,doc of ca in hashmap for O(1) search
			HashMap<Object,BasicDBObject> map_ca = new HashMap<Object,BasicDBObject>();
			DBCursor cur_ss = store_sales.find();

			int count=0;
			
			while(cur_ss.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss.next();
		
				if(map_ca.containsKey(val.get("ss_addr_sk"))){
					store_sales.update(new BasicDBObject("ss_addr_sk",val.get("ss_addr_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_ca.get(val.get("ss_addr_sk")))));					
				}
				else{
					BasicDBObject query_ca = new BasicDBObject("ca_address_sk",val.get("ss_addr_sk"));
					DBCursor cur_ca = address.find(query_ca);
					while(cur_ca.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_ca.next();
						doc.removeField("_id");
						//doc.removeField("ca_address_sk");
						map_ca.put(val.get("ss_addr_sk"), doc);

						store_sales.update(new BasicDBObject("ss_addr_sk",val.get("ss_addr_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_ca.get(val.get("ss_addr_sk")))));				
					}
				
				}
				count++;
				System.out.println(count);
			}
			
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time for partial querying (with embedding): "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		
			
			map_ca.clear();
		}
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



