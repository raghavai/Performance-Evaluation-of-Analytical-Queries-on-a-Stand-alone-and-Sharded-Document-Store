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


public class Query21_embed_loop {
	public static void main(String args[]){
		
		try{
			
			long startTime = System.currentTimeMillis();

			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection inventory = db.getCollection("partial_result_q14_agg");
			DBCollection item = db.getCollection("item");
			DBCollection warehouse = db.getCollection("warehouse");
			DBCollection date = db.getCollection("date_dim");
			
			HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_w = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_d = new HashMap<Object,BasicDBObject>();
			
			long stime, etime,mill;
			
			DBCursor cur_inv = inventory.find();
			int count=0;
			int map_cnt = 0;
			int no_map_cnt = 0;
			int m_cnt=0;
			int no_m_cnt = 0;

			//-------------------------item----------------------------------//

			stime = System.currentTimeMillis();
			while(cur_inv.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_inv.next();
				count++;
				System.out.println(count);
				
				if(map_i.containsKey(val.get("inv_item_sk"))){
					//map_cnt++;
					inventory.update(new BasicDBObject("inv_item_sk",val.get("inv_item_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_item_sk",map_i.get(val.get("inv_item_sk")))));		
				}
				else{
					//no_map_cnt++;
					BasicDBObject query_i = new BasicDBObject("i_item_sk",val.get("inv_item_sk"));
					DBCursor cur_i = item.find(query_i);
					while(cur_i.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_i.next();
						doc.removeField("_id");
						doc.removeField("i_item_sk");
						map_i.put(val.get("inv_item_sk"), doc);

						inventory.update(new BasicDBObject("inv_item_sk",val.get("inv_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_item_sk",map_i.get(val.get("inv_item_sk")))));				
	
					}
				}
		
				if(map_w.containsKey(val.get("inv_warehouse_sk"))){
					//m_cnt++;
					inventory.update(new BasicDBObject("inv_warehouse_sk",val.get("inv_warehouse_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_warehouse_sk",map_w.get(val.get("inv_warehouse_sk")))));		
				}
				else{
					//no_m_cnt++;
					BasicDBObject query_w = new BasicDBObject("w_warehouse_sk",val.get("inv_warehouse_sk"));
					DBCursor cur_w = warehouse.find(query_w);
					while(cur_w.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_w.next();
						doc.removeField("_id");
						doc.removeField("w_warehouse_sk");
						map_w.put(val.get("inv_warehouse_sk"), doc);

						inventory.update(new BasicDBObject("inv_warehouse_sk",val.get("inv_warehouse_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_warehouse_sk",map_w.get(val.get("inv_warehouse_sk")))));				
	
					}
				}
			
					
				if(map_d.containsKey(val.get("inv_date_sk"))){
					//map_cnt++;
					inventory.update(new BasicDBObject("inv_date_sk",val.get("inv_date_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_date_sk",map_d.get(val.get("inv_date_sk")))));		
				}
				else{
					//no_map_cnt++;
					BasicDBObject query_d = new BasicDBObject("d_date_sk",val.get("inv_date_sk"));
					DBCursor cur_d = date.find(query_d);
					while(cur_d.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_d.next();
						doc.removeField("_id");
						doc.removeField("d_date_sk");
						map_d.put(val.get("inv_date_sk"), doc);

						inventory.update(new BasicDBObject("inv_date_sk",val.get("inv_date_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_date_sk",map_d.get(val.get("inv_date_sk")))));				
	
					}
				}
			}
			
			/*System.out.println("map size "+map_i.size());
			System.out.println("map count "+map_cnt);
			System.out.println("no map count "+no_map_cnt);
			
			System.out.println("map size "+map_w.size());
			System.out.println("map count "+m_cnt);
			System.out.println("no map count "+no_m_cnt);
			*/
				
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time for partial querying (with embedding): "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		

		}
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



