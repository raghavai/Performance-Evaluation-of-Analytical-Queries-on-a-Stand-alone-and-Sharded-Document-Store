package FullEmbed_docs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class Embed_SS {
	
	public Embed_SS(DB db_, List<HashMap<Object,BasicDBObject>> list_map, HashMap<Object,BasicDBObject> map_cd){
		
		DB db = db_;
		HashMap<Object,BasicDBObject> map_s = list_map.get(0);
		HashMap<Object,BasicDBObject> map_t = list_map.get(1);
		HashMap<Object,BasicDBObject> map_d = list_map.get(2);
		HashMap<Object,BasicDBObject> map_i = list_map.get(3);
		HashMap<Object,BasicDBObject> map_p = list_map.get(4);
		HashMap<Object,BasicDBObject> map_c = list_map.get(5);
		HashMap<Object,BasicDBObject> map_ca = list_map.get(6);
		HashMap<Object,BasicDBObject> map_hd = list_map.get(7);

		BasicDBObject query = new BasicDBObject();
		
		DBCollection store_sales = db.getCollection("store_sales_embed");
		System.out.println("In the embedding code");
		int count = 0;
		
	    List<BasicDBObject> list = new ArrayList<BasicDBObject>();
	    list.add(new BasicDBObject("ss_store_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_sold_time_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_sold_date_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_item_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_promo_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_customer_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_addr_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_hdemo_sk", new BasicDBObject("$exists",true)));
	    list.add(new BasicDBObject("ss_cdemo_sk", new BasicDBObject("$exists",true)));
	    query.put("$or", list);
	    DBCursor cur_ss = store_sales.find(query);

	    while(cur_ss.hasNext()){
	    	
	    	int cnt_s=0,cnt_t=0,cnt_d=0,cnt_i=0,cnt_p=0,cnt_c=0,cnt_ca=0,cnt_hd=0,cnt_cd=0;
	    	BasicDBObject val = (BasicDBObject) cur_ss.next();
	    	
	    	//-----------Store-------------------//
	 
	    	if(val.get("ss_store_sk")!=null){
	    		if(map_s.containsKey(val.get("ss_store_sk"))){
					cnt_s++;
					store_sales.update(new BasicDBObject("ss_store_sk",val.get("ss_store_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_store_sk",map_s.get(val.get("ss_store_sk")))));						
				}
	    	}
	    	
			//--------Time_Dim---------------//
	
			
			if(val.get("ss_sold_time_sk")!=null){
				if(map_t.containsKey(val.get("ss_sold_time_sk"))){
					cnt_t++;
	
					store_sales.update(new BasicDBObject("ss_sold_time_sk",val.get("ss_sold_time_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_sold_time_sk",map_t.get(val.get("ss_sold_time_sk")))));						
				}
			}
					
			//--------Date_Dim---------------//
	
		
			if(val.get("ss_sold_date_sk")!=null){
				if(map_d.containsKey(val.get("ss_sold_date_sk"))){
						cnt_d++;
		
						store_sales.update(new BasicDBObject("ss_sold_date_sk",val.get("ss_sold_date_sk")),
									new BasicDBObject("$set", new BasicDBObject("ss_sold_date_sk",map_d.get(val.get("ss_sold_date_sk")))));						
				}
			}
			
			//--------Item---------------//
	
			
			if(val.get("ss_item_sk")!=null){
				if(map_i.containsKey(val.get("ss_item_sk"))){
					cnt_i++;
	
					store_sales.update(new BasicDBObject("ss_item_sk",val.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map_i.get(val.get("ss_item_sk")))));						
				}
			}
			
			//--------Promotion---------------//
	
			
			if(val.get("ss_promo_sk")!=null){
				if(map_p.containsKey(val.get("ss_promo_sk"))){
					cnt_p++;
	
					store_sales.update(new BasicDBObject("ss_promo_sk",val.get("ss_promo_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_promo_sk",map_p.get(val.get("ss_promo_sk")))));						
				}
			}
			
			//--------Customer_Demographics---------------//
	
			//HashMap<Object,BasicDBObject> map_cd = map_cd;
			if(val.get("ss_cdemo_sk")!=null){
				if(map_cd.containsKey(val.get("ss_cdemo_sk"))){
					cnt_cd++;
	
					store_sales.update(new BasicDBObject("ss_cdemo_sk",val.get("ss_cdemo_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_cdemo_sk",map_cd.get(val.get("ss_cdemo_sk")))));						
				}
			}
			
			//--------Customer---------------//
	
			
			if(val.get("ss_customer_sk")!=null){
				if(map_c.containsKey(val.get("ss_customer_sk"))){
					cnt_c++;
	
					store_sales.update(new BasicDBObject("ss_customer_sk",val.get("ss_customer_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_c.get(val.get("ss_customer_sk")))));						
				}
			}
			
	
			//--------Customer_address---------------//
	
			
			if(val.get("ss_addr_sk")!=null){
				if(map_ca.containsKey(val.get("ss_addr_sk"))){
					cnt_ca++;
	
					store_sales.update(new BasicDBObject("ss_addr_sk",val.get("ss_addr_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_ca.get(val.get("ss_addr_sk")))));						
				}
			}
			
			//--------Household_demographics---------------//
	
			
			if(val.get("ss_hdemo_sk")!=null){		
				if(map_hd.containsKey(val.get("ss_hdemo_sk"))){
					cnt_hd++;
	
					store_sales.update(new BasicDBObject("ss_hdemo_sk",val.get("ss_hdemo_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_hdemo_sk",map_hd.get(val.get("ss_hdemo_sk")))));						
					}
				}
		    
			System.out.println("cnt_s:"+cnt_s+" cnt_t:"+cnt_t+" cnt_d:"+cnt_d+" cnt_i:"+cnt_i+" cnt_p:"+cnt_p+" cnt_c:"+cnt_c+" cnt_ca:"
					+cnt_ca+" cnt_hd:"+cnt_hd+" cnt_cd:"+cnt_cd);
			count++;
			System.out.println("doc# :"+count);
	    
	    	}
	    
	    map_s.clear(); map_t.clear(); map_d.clear(); map_i.clear(); map_p.clear();
	    map_cd.clear(); map_c.clear(); map_ca.clear(); map_hd.clear();
		}
	}
