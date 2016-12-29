/*
 * Cursor for individual collections
 * Cursor for semi-join on fact table 
 * Create individual indexes on inv_date_sk, inv_warehouse_sk, inv_item_sk
 * Create maps on the fly and embed the documents
 * Aggregation for final output of query 
 */

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


public class Query21_full_cursor {
	public static void main(String args[]){
		
		try{
			
			long startTime,endTime,millis,millis1,millis2;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("TestRun_S");
					
			DBCollection date = db.getCollection("date_dim");
			DBCollection item = db.getCollection("item");
			DBCollection warehouse = db.getCollection("warehouse");
			DBCollection inventory = db.getCollection("inventory");
			DBCollection q14_partial = db.createCollection("q14_partial_cur", null);
			DBCollection q14_final = db.createCollection("q14_final_cur", null);

			
			BasicDBObject temp = new BasicDBObject();
			BasicDBObject query = new BasicDBObject();
		    List<BasicDBObject> list = new ArrayList<BasicDBObject>();
			HashMap<Object,BasicDBObject> map_d = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_w = new HashMap<Object,BasicDBObject>();
		    List<Object> arr_d = new ArrayList<Object>();
		    List<Object> arr_i = new ArrayList<Object>();
		    List<Object> arr_w = new ArrayList<Object>();
		    
		    		    
		    //-----------Date_dim---------//
		    
		    list.add(new BasicDBObject("d_date", new BasicDBObject("$gte","2002-04-29")));
		    list.add(new BasicDBObject("d_date", new BasicDBObject("$lte","2002-06-28")));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_d = date.find(query);
		    while(cur_d.hasNext()){
				temp = (BasicDBObject) cur_d.next();
		    	arr_d.add(temp.get("d_date_sk"));
		    
		    	//-----putting k-v pair in hashmap-----//
		    	BasicDBObject doc = (BasicDBObject) temp.copy();
				doc.removeField("_id");
				//doc.put("_id", new ObjectId());
				doc.removeField("d_date_sk");
				map_d.put(temp.get("d_date_sk"), doc);
		    }
		    temp.clear();
		    list.clear();
		    query.clear();
		    System.out.println("date map"+arr_d.size());
		    

		    //----------Item---------//
		    
		    list.add(new BasicDBObject("i_current_price", new BasicDBObject("$gte",0.99)));
		    list.add(new BasicDBObject("i_current_price", new BasicDBObject("$lte",1.49)));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_i = item.find(query);
		    while(cur_i.hasNext()){
				temp = (BasicDBObject) cur_i.next();
		    	arr_i.add(temp.get("i_item_sk"));
		    
		    	//-----putting k-v pair in hashmap-----//
		    	BasicDBObject doc = (BasicDBObject) temp.copy();
				doc.removeField("_id");
				//doc.put("_id", new ObjectId());
				doc.removeField("i_item_sk");
				map_i.put(temp.get("i_item_sk"), doc);
		    }
		    temp.clear();
		    list.clear();
		    query.clear();
		    System.out.println("item map"+arr_i.size());
		    
		    
		    //-------Warehouse----------//
		    
		    DBCursor cur_w = warehouse.find();
		    while(cur_w.hasNext()){
				temp = (BasicDBObject) cur_w.next();
		    	arr_w.add(temp.get("w_warehouse_sk"));
		    	
		    	//----putting k,v pair in hashmap-----//
				BasicDBObject doc = (BasicDBObject) temp.copy();
				doc.removeField("_id");
				//doc.put("_id", new ObjectId());
				doc.removeField("w_warehouse_sk");
				map_w.put(temp.get("w_warehouse_sk"), doc);
		    }
		    temp.clear();
		    System.out.println("warehouse map"+arr_w.size());
		    
		    
		    //----------Query inventory----------//
		    
		    list.add(new BasicDBObject("inv_date_sk", new BasicDBObject("$in",arr_d)));
		    list.add(new BasicDBObject("inv_item_sk", new BasicDBObject("$in",arr_i)));
		    list.add(new BasicDBObject("inv_warehouse_sk", new BasicDBObject("$in",arr_w)));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_inv = inventory.find(query);
		    int count = 0;
		    
		    q14_partial.createIndex(new BasicDBObject("inv_item_sk",1));
	    	q14_partial.createIndex(new BasicDBObject("inv_date_sk",1));
	    	q14_partial.createIndex(new BasicDBObject("inv_warehouse_sk",1));
	    	
		    while(cur_inv.hasNext()){
				BasicDBObject temp1 = (BasicDBObject) cur_inv.next();
		    	q14_partial.insert(temp1);
		    	
		    	
		    	//-----embed the date doc on the fly----//
		    	if(map_d.containsKey(temp1.get("inv_date_sk"))){
					q14_partial.update(new BasicDBObject("inv_date_sk",temp1.get("inv_date_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_date_sk",map_d.get(temp1.get("inv_date_sk")))));					
				}
		    	
		    	//------embed item doc on the fly-------//
		    	if(map_i.containsKey(temp1.get("inv_item_sk"))){
					q14_partial.update(new BasicDBObject("inv_item_sk",temp1.get("inv_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_item_sk",map_i.get(temp1.get("inv_item_sk")))));					
				}
		    	
		    	//------embed warehouse doc on the fly-------//
		    	if(map_w.containsKey(temp1.get("inv_warehouse_sk"))){
					q14_partial.update(new BasicDBObject("inv_warehouse_sk",temp1.get("inv_warehouse_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_warehouse_sk",map_w.get(temp1.get("inv_warehouse_sk")))));					
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
		    
			
			
			//-----------final aggregation query on q14_partial------------//
			
			startTime = System.currentTimeMillis();
			
			//--------Grouping---------//
			
			DBObject groupIdKeys = new BasicDBObject( "w_name", "$inv_warehouse_sk.w_warehouse_name");
			groupIdKeys.put("i_id","$inv_item_sk.i_item_id");
			
			/*	inv_before	*/
			List<Object> ltOperands = new ArrayList<Object>();
			ltOperands.add("$inv_date_sk.d_date");
			ltOperands.add("2002-05-29");
			
			List<Object> invBefore_cond = new ArrayList<Object>();
			invBefore_cond.add(new BasicDBObject("$lt",ltOperands));
			invBefore_cond.add("$inv_quantity_on_hand");
			invBefore_cond.add(0);
			BasicDBObject finalinvBefore_cond = new BasicDBObject("$cond",invBefore_cond);
			
			DBObject invBefore = new BasicDBObject("$sum",finalinvBefore_cond);
			
			/*	inv_after	*/
			List<Object> gteOperands = new ArrayList<Object>();
			gteOperands.add("$inv_date_sk.d_date");
			gteOperands.add("2002-05-29");
			
			List<Object> invAfter_cond = new ArrayList<Object>();
			invAfter_cond.add(new BasicDBObject("$gte",gteOperands));
			invAfter_cond.add("$inv_quantity_on_hand");
			invAfter_cond.add(0);
			BasicDBObject finalinvAfter_cond = new BasicDBObject("$cond",invAfter_cond);
			
			DBObject invAfter = new BasicDBObject("$sum",finalinvAfter_cond);
			
			/*	bringing all together   */
			
			DBObject groupField = new BasicDBObject("_id",groupIdKeys);
			groupField.put("inv_before", invBefore);
			groupField.put("inv_after", invAfter);
			
			DBObject group = new BasicDBObject("$group", groupField);
			
			
			//----------First projection-----------//
			
			List<Object> divOperand = new ArrayList<Object>();
			divOperand.add("$inv_before");
			divOperand.add("$inv_after");
			
			DBObject projectField_1 = new BasicDBObject("_id",1);
			projectField_1.put("temp", new BasicDBObject("$divide",divOperand));
			projectField_1.put("inv_before", 1);
			projectField_1.put("inv_after", 1);
			
			DBObject project_1 = new BasicDBObject("$project",projectField_1);
			
			
			//---------Matching-------------------//

			DBObject match = new BasicDBObject("$match",
					new BasicDBObject("temp",
					new BasicDBObject("$gte",2.0/3.0).append("$lte",3.0/2.0)));
			
			
			//-------Second Projection----------//
			
			DBObject projectField_2 = new BasicDBObject("_id",0);
			projectField_2.put("w_warehouse_name", "$_id.w_name");
			projectField_2.put("i_item_id", "$_id.i_id");
			projectField_2.put("inv_before", 1);
			projectField_2.put("inv_after", 1);
			
			DBObject project_2 = new BasicDBObject("$project",projectField_2);
			
			
			//---------Sorting---------------------//
			
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject("w_warehouse_name",1).append("i_item_id", 1));
			
	
			//------run aggregation-------------//
			
			List<DBObject> pipeline = Arrays.asList(group, project_1, match, project_2, sort);
			AggregationOutput output = q14_partial.aggregate(pipeline);
			
			//view final collection 
			for(DBObject result : output.results()){
				q14_final.insert(result);
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



