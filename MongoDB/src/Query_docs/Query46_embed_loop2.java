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


public class Query46_embed_loop2 {
	public static void main(String args[]){
		
		try{
			long startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection q23 = db.getCollection("partial_result3_q23_agg");
			DBCollection cust_addr = db.getCollection("cust_addr");
			
			//Store all the PK,doc of ca in hashmap for O(1) search
			HashMap<Object,BasicDBObject> map_ca = new HashMap<Object,BasicDBObject>();
			DBCursor cur_q23 = q23.find();

			int count=0;
			
			while(cur_q23.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_q23.next();
		
				if(map_ca.containsKey(val.get("ss_customer_sk"))){
					q23.update(new BasicDBObject("ss_customer_sk",val.get("ss_customer_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_ca.get(val.get("ss_customer_sk")))));					
				}
				else{
					BasicDBObject query_ca = new BasicDBObject("c_customer_sk",val.get("ss_customer_sk"));
					DBCursor cur_ca = cust_addr.find(query_ca);
					while(cur_ca.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_ca.next();
						doc.removeField("_id");
						//doc.removeField("ca_address_sk");
						map_ca.put(val.get("ss_customer_sk"), doc);
					
						q23.update(new BasicDBObject("ss_customer_sk",val.get("ss_customer_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_ca.get(val.get("ss_customer_sk")))));
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



