package FullEmbed_docs;

import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class CreateMap {

	public CreateMap(List<DBCollection> list_coll, List<HashMap<Object,BasicDBObject>> list_map) {
		
		int count = 0;
		
		//--------createMap for Store-------//
		HashMap<Object,BasicDBObject> map_s = list_map.get(0);
		DBCollection store = list_coll.get(0);
		DBCursor cur_s = store.find();
		while(cur_s.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_s.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("s_store_sk");
			map_s.put(val.get("s_store_sk"), doc);
		}
		for(Object key:map_s.keySet()){
			System.out.println(key+" "+map_s.get(key));
		}
		System.out.println("Total maps in store: "+count);
		
		count=0;
		
		//--------createMap for Time_Dim-------//
		HashMap<Object,BasicDBObject> map_t = list_map.get(1);
		DBCollection time = list_coll.get(1);
		DBCursor cur_t = time.find();
		while(cur_t.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_t.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("t_time_sk");
			map_t.put(val.get("t_time_sk"), doc);
		}
		System.out.println("Total maps in time_dim: "+count);
		
		count=0;
				
		//--------createMap for Date_Dim-------//
		HashMap<Object,BasicDBObject> map_d = list_map.get(2);
		DBCollection date = list_coll.get(2);
		DBCursor cur_d = date.find();
		while(cur_d.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_d.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("d_date_sk");
			map_d.put(val.get("d_date_sk"), doc);
		}
		System.out.println("Total maps in date_dim: "+count);
		
		count=0;
		
		
		//--------createMap for Item-------//
		HashMap<Object,BasicDBObject> map_i = list_map.get(3);
		DBCollection item = list_coll.get(3);
		DBCursor cur_i = item.find();
		while(cur_i.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_i.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("i_item_sk");
			map_i.put(val.get("i_item_sk"), doc);
		}
		System.out.println("Total maps in item: "+count);
		
		count=0;
		
		
		//--------createMap for Promotion-------//
		HashMap<Object,BasicDBObject> map_p = list_map.get(4);
		DBCollection promo = list_coll.get(4);
		DBCursor cur_p = promo.find();
		while(cur_p.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_p.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("p_promo_sk");
			map_p.put(val.get("p_promo_sk"), doc);
		}
		System.out.println("Total maps in promotion: "+count);
		
		count=0;
		
		//--------createMap for Customer-------//
		HashMap<Object,BasicDBObject> map_c = list_map.get(5);
		DBCollection cust = list_coll.get(5);
		DBCursor cur_c = cust.find();
		while(cur_c.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_c.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("c_customer_sk");
			map_c.put(val.get("c_customer_sk"), doc);
		}
		System.out.println("Total maps in customer: "+count);
		
		count=0;
		
		//--------createMap for Customer_Address-------//
		HashMap<Object,BasicDBObject> map_ca = list_map.get(6);
		DBCollection cust_addr = list_coll.get(6);
		DBCursor cur_ca = cust_addr.find();
		while(cur_ca.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_ca.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("ca_address_sk");
			map_ca.put(val.get("ca_address_sk"), doc);
		}
		System.out.println("Total maps in customer_address: "+count);
		
		count=0;
		
		//--------createMap for Householde_Demographics-------//
		HashMap<Object,BasicDBObject> map_hd = list_map.get(7);
		DBCollection house_demo = list_coll.get(7);
		DBCursor cur_hd = house_demo.find();
		while(cur_hd.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_hd.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("hd_demo_sk");
			map_hd.put(val.get("hd_demo_sk"), doc);
		}	
		System.out.println("Total maps in household_demographics: "+count);
		
	}
}
