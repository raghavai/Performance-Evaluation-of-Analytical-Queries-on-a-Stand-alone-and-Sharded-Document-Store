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


public class Query46_embed {
	public static void main(String args[]){
		
		try{
			long startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection store_sales = db.getCollection("pr_q23_agg");
			DBCollection cust_addr = db.getCollection("cust_addr");
			DBCollection address = db.getCollection("customer_address"); 
			
			//Store all the PK,doc of ca in hashmap for O(1) search
			HashMap<Object,BasicDBObject> map_cust_addr = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_address = new HashMap<Object,BasicDBObject>();
			DBCursor cur_ss = store_sales.find();

			store_sales.createIndex(new BasicDBObject("ss_customer_sk.c_customer_sk",1));
			store_sales.createIndex(new BasicDBObject("ss_addr_sk.ca_address_sk",1));
			
			int count=0;
			
			while(cur_ss.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss.next();
				Object ss_customer_sk = val.get("ss_customer_sk");
				//-----embed the embedded customer..address documents----//
				
				if(map_cust_addr.containsKey(ss_customer_sk)){
					store_sales.update(new BasicDBObject("ss_customer_sk",ss_customer_sk),
								new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_cust_addr.get(ss_customer_sk))));					
				}
				else{
					BasicDBObject query_cust_addr = new BasicDBObject("c_customer_sk",ss_customer_sk);
					DBCursor cur_cust_addr = cust_addr.find(query_cust_addr);
					int cnt = 0;
					while(cur_cust_addr.hasNext()){
						cnt++;
						System.out.println("count cursor :"+cnt);
						BasicDBObject doc1 = (BasicDBObject) cur_cust_addr.next();
						doc1.removeField("_id");
						//doc.removeField("ca_address_sk");
						map_cust_addr.put(ss_customer_sk, doc1);

						store_sales.update(new BasicDBObject("ss_customer_sk",ss_customer_sk),
								new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_cust_addr.get(ss_customer_sk))));				
					}
				}
				count++;
				System.out.println(count);
			}
				
			count =0;
			DBCursor cur_ss1 = store_sales.find();

			while(cur_ss1.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss1.next();
				Object ss_addr_sk = val.get("ss_addr_sk");
				//-----embed only customer address documents-------//
				
				if(map_address.containsKey(ss_addr_sk)){
					store_sales.update(new BasicDBObject("ss_addr_sk",ss_addr_sk),
								new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_address.get(ss_addr_sk))));					
				}
				else{
					BasicDBObject query_address = new BasicDBObject("ca_address_sk",ss_addr_sk);
					DBCursor cur_address = address.find(query_address);
					while(cur_address.hasNext()){
						BasicDBObject doc2 = (BasicDBObject) cur_address.next();
						doc2.removeField("_id");
						//doc.removeField("ca_address_sk");
						map_address.put(ss_addr_sk, doc2);

						store_sales.update(new BasicDBObject("ss_addr_sk",ss_addr_sk),
								new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_address.get(ss_addr_sk))));				
					}
				
				}	
				count++;
				System.out.println(count);
			}
			
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time for partial querying (with embedding): "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		
			
			map_cust_addr.clear();
			map_address.clear();
		}
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



