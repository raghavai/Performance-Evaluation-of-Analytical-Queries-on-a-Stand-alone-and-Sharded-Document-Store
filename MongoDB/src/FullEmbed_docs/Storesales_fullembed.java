package FullEmbed_docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class Storesales_fullembed {
	
	public static void main(String args[]){
		
		try{
			
			long startTime,endTime,millis;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			//DBCollection store_sales = db.getCollection("store_sales");
			DBCollection store = db.getCollection("store");
			DBCollection time = db.getCollection("time_dim");
			DBCollection date = db.getCollection("date_dim");
			DBCollection item = db.getCollection("item");
			DBCollection promo = db.getCollection("promotion");
			//DBCollection cust_demo = db.getCollection("customer_demographics");
			DBCollection cust = db.getCollection("customer");
			DBCollection cust_addr = db.getCollection("customer_address");
			DBCollection house_demo = db.getCollection("household_demographics");
			
			//--------------------Creating Maps------------------------//
			
			List<DBCollection> list_coll = new ArrayList<DBCollection>();
			list_coll.add(store); list_coll.add(time); list_coll.add(date); list_coll.add(item); 
			list_coll.add(promo); list_coll.add(cust); list_coll.add(cust_addr); list_coll.add(house_demo);
			
			
			HashMap<Object,BasicDBObject> map_s = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_t = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_d = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_p = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_cd = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_c = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_ca = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_hd = new HashMap<Object,BasicDBObject>();
			
			List<HashMap<Object,BasicDBObject>> list_map = new ArrayList<HashMap<Object,BasicDBObject>>();
			list_map.add(map_s); list_map.add(map_t); list_map.add(map_d); list_map.add(map_i); 
			list_map.add(map_p); list_map.add(map_c); list_map.add(map_ca); list_map.add(map_hd);

			new CreateMap(list_coll,list_map);
			new CreateMap_CD(db,map_cd);
		
			endTime = System.currentTimeMillis();
			millis = endTime-startTime;
			System.out.println("Total time for creating maps: "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		
			
			
			//----------embedding store_sales collection------------------//
			startTime = System.currentTimeMillis();

			new Embed_SS(db,list_map,map_cd);
			
			endTime = System.currentTimeMillis();
			millis = endTime-startTime;
			System.out.println("Total time for embedding: "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");		
			
			
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}






