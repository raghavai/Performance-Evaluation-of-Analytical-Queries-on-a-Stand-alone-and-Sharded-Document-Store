package Query_docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class Query7_full_cursor {
	public static void main(String args[]){
		
		try{
			
			long startTime,endTime,millis,millis1,millis2;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection cust_demo = db.getCollection("customer_demographics");
			DBCollection promo = db.getCollection("promotion");
			DBCollection date = db.getCollection("date_dim");
			DBCollection item = db.getCollection("item");
			DBCollection store_sales = db.getCollection("store_sales");
			DBCollection q2_partial = db.createCollection("q2_partial_cur", null);
			DBCollection q2_final = db.createCollection("q2_final_cur", null);

			
			BasicDBObject query = new BasicDBObject();
		    List<BasicDBObject> list = new ArrayList<BasicDBObject>();
			HashMap<Object,BasicDBObject> map = new HashMap<Object,BasicDBObject>();
		    List<Object> arr_cd = new ArrayList<Object>();
		    List<Object> arr_p = new ArrayList<Object>();
		    List<Object> arr_d = new ArrayList<Object>();
		    List<Object> arr_i = new ArrayList<Object>();
		    
		    //-------Customer_demographics----------//
		    list.add(new BasicDBObject("cd_gender", "M"));
		    list.add(new BasicDBObject("cd_education_status", "4 yr Degree"));
		    list.add(new BasicDBObject("cd_marital_status", "M"));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_cd = cust_demo.find(query);
		    while(cur_cd.hasNext()){
		    	arr_cd.add(cur_cd.next().get("cd_demo_sk"));
		    }
		    System.out.println(arr_cd.size());
		    list.clear();
		    query.clear();
		    
		    //-------promotion----------//
		    list.add(new BasicDBObject("p_channel_email", "N"));
		    list.add(new BasicDBObject("p_channel_event", "N"));
		    query.put("$or", list);
		    System.out.println(query.toString());
		    DBCursor cur_p = promo.find(query);
		    while(cur_p.hasNext()){
		    	arr_p.add(cur_p.next().get("p_promo_sk"));
		    }
		    System.out.println(arr_p.size());
		    list.clear();
		    query.clear();
		  
		    //-------date_dim----------//
		    query.put("d_year", 2001);
		    System.out.println(query.toString());
		    DBCursor cur_d = date.find(query);
		    while(cur_d.hasNext()){
		    	arr_d.add(cur_d.next().get("d_date_sk"));
		    }
		    System.out.println(arr_d.size());
		    query.clear();
		  
		    //-------item----------//
		    DBCursor cur_i = item.find();
		    while(cur_i.hasNext()){
				BasicDBObject temp1 = (BasicDBObject) cur_i.next();
		    	arr_i.add(temp1.get("i_item_sk"));
		    	
		    	//----putting k,v pair in hashmap
				BasicDBObject doc = (BasicDBObject) temp1.copy();
				doc.removeField("_id");
				//doc.put("_id", new ObjectId());
				doc.removeField("i_item_sk");
				map.put(temp1.get("i_item_sk"), doc);
		    }
		    
		    System.out.println(arr_i.size());
		    
		    //----------Query store_sales----------//
		    list.add(new BasicDBObject("ss_cdemo_sk", new BasicDBObject("$in",arr_cd)));
		    list.add(new BasicDBObject("ss_promo_sk", new BasicDBObject("$in",arr_p)));
		    list.add(new BasicDBObject("ss_sold_date_sk", new BasicDBObject("$in",arr_d)));
		    list.add(new BasicDBObject("ss_item_sk", new BasicDBObject("$in",arr_i)));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_ss = store_sales.find(query);
		    int count = 0;

		    q2_partial.createIndex(new BasicDBObject("ss_item_sk",1));

		    while(cur_ss.hasNext()){
				BasicDBObject temp2 = (BasicDBObject) cur_ss.next();
		    	q2_partial.insert(temp2);
		    	
		    	//-----embed the item docs on the fly----//
		    	if(map.containsKey(temp2.get("ss_item_sk"))){
					q2_partial.update(new BasicDBObject("ss_item_sk",temp2.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map.get(temp2.get("ss_item_sk")))));					
				}
		    	count++;
		    	System.out.println(count);
		    }
		    list.clear();
		    query.clear();
		  
		    endTime = System.currentTimeMillis();
			millis1 = endTime-startTime;
			//System.out.println("Total time for partial querying (with embedding): "+((millis1/1000)/60)+" minutes "+((millis1 / 1000) % 60)+" seconds");
			System.out.println("Total time for partial querying (with embedding): "+millis1+" milliseconds");
		    
			
			
			//-----------final aggregation query on q2_partial------------//
			startTime = System.currentTimeMillis();
			
			DBObject groupField = new BasicDBObject( "_id", "$ss_item_sk.i_item_id");
			groupField.put("agg1", new BasicDBObject( "$avg", "$ss_quantity"));
			groupField.put("agg2", new BasicDBObject( "$avg", "$ss_list_price"));
			groupField.put("agg3", new BasicDBObject( "$avg", "$ss_coupon_amt"));
			groupField.put("agg4", new BasicDBObject( "$avg", "$ss_sales_price"));
			DBObject group = new BasicDBObject("$group", groupField);

			// Finally the $sort operation
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject("ss_item_sk.i_item_id", 1));

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(group, sort);
			AggregationOutput output = q2_partial.aggregate(pipeline);
			
			//view final collection 
			for(DBObject result : output.results()){
				q2_final.insert(result);
			}
			
			endTime = System.currentTimeMillis();
			millis2 = endTime-startTime;
			//System.out.println("Total time for aggregation: "+((millis2/1000)/60)+" minutes "+((millis2 / 1000) % 60)+" seconds");
			System.out.println("Total time for aggregation: "+millis2+" milliseconds");
			
			millis = millis1 + millis2;
			//System.out.println("Total time to run query: "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");
			System.out.println("Total time to run query: "+millis+" milliseconds");
			}
		
		catch(IOException e){
			e.printStackTrace();
		} 
	}
}



