/*
 * cust_addr is a copy of the customer collection
 * Before you execute create an index on  
 * c_current_addr_sk for cust_addr collection &
 * ca_address_sk for customer_address collection
 */
package FullEmbed_docs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class Cust_Custaddress {
	public static void main(String args[]){
		try{

			long startTime = System.currentTimeMillis();

			MongoClient mongoClient = new MongoClient("192.168.2.159",27017);
			DB db = mongoClient.getDB("Dataset_5GB");
			
			DBCollection customer = db.getCollection("cust_addr");
			DBCollection address = db.getCollection("customer_address");
			
			//Store all the PK,doc of ca in hashmap for O(1) search
			HashMap<Object,BasicDBObject> map_ca = new HashMap<Object,BasicDBObject>();
			DBCursor cur_c = customer.find();
					
			int count=0;

			while(cur_c.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_c.next();
				
				if(map_ca.containsKey(val.get("c_current_addr_sk"))){
					customer.update(new BasicDBObject("c_current_addr_sk",val.get("c_current_addr_sk")),
								new BasicDBObject("$set", new BasicDBObject("c_current_addr_sk",map_ca.get(val.get("c_current_addr_sk")))));					
				}
				else{
					BasicDBObject query_ca = new BasicDBObject("ca_address_sk",val.get("c_current_addr_sk"));
					DBCursor cur_ca = address.find(query_ca);
					while(cur_ca.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_ca.next();
						doc.removeField("_id");
						//doc.removeField("ca_address_sk");
						map_ca.put(val.get("c_current_addr_sk"), doc);

						customer.update(new BasicDBObject("c_current_addr_sk",val.get("c_current_addr_sk")),
								new BasicDBObject("$set", new BasicDBObject("c_current_addr_sk",map_ca.get(val.get("c_current_addr_sk")))));				
					}
				
				}
				count++;
				System.out.println(count);
			}
				
			
			long endTime = System.currentTimeMillis();
			long millis = endTime-startTime;
			System.out.println("Total time to embed documents: "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");			
			map_ca.clear();
			
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}


