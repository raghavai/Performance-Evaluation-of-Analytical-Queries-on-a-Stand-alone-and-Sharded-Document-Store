/*
Selective Mapping
Individual indexes on inv_date_sk, inv_warehouse_sk, inv_item_sk
Many loops for all individual conditions
*/

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


public class Query50_embed_loop {
	public static void main(String args[]){
		
		try{
			
			long startTime = System.currentTimeMillis();

			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection store_sales = db.getCollection("q60_ss");
			DBCollection store_returns = db.getCollection("q60_sr");
			DBCollection store = db.getCollection("store");
			
			HashMap<Object,BasicDBObject> map_sr = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_s = new HashMap<Object,BasicDBObject>();

			long stime, etime,mill;
			
			DBCursor cur_ss = store_sales.find();
			int count=0;
			//-------------------------item----------------------------------//

			stime = System.currentTimeMillis();
			while(cur_ss.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss.next();
				count++;
				System.out.println(count);
				
				if(map_sr.containsKey(val.get("ss_keys"))){
					//map_cnt++;
					store_sales.update(new BasicDBObject("ss_keys",val.get("ss_keys")),
							new BasicDBObject("$set", new BasicDBObject("ss_keys",map_sr.get(val.get("ss_keys")))));		
				}
				else{
					//no_map_cnt++;
					BasicDBObject query_sr = new BasicDBObject("sr_keys",val.get("ss_keys"));
					DBCursor cur_sr = store_returns.find(query_sr);
					while(cur_sr.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_sr.next();
						doc.removeField("_id");
						map_sr.put(val.get("ss_keys"), doc);

						store_sales.update(new BasicDBObject("ss_keys",val.get("ss_keys")),
								new BasicDBObject("$set", new BasicDBObject("ss_keys",map_sr.get(val.get("ss_keys")))));				
	
					}
				}
				
				if(map_s.containsKey(val.get("ss_store_sk"))){
					//map_cnt++;
					store_sales.update(new BasicDBObject("ss_store_sk",val.get("ss_store_sk")),
							new BasicDBObject("$set", new BasicDBObject("ss_store_sk",map_s.get(val.get("ss_store_sk")))));		
				}
				else{
					//no_map_cnt++;
					BasicDBObject query_s = new BasicDBObject("s_store_sk",val.get("ss_store_sk"));
					DBCursor cur_s = store.find(query_s);
					while(cur_s.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_s.next();
						doc.removeField("_id");
						map_s.put(val.get("ss_store_sk"), doc);

						store_sales.update(new BasicDBObject("ss_store_sk",val.get("ss_store_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_store_sk",map_s.get(val.get("ss_store_sk")))));				
	
					}
				}
			}
		
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time for partial querying (with embedding): "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		

		}
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



