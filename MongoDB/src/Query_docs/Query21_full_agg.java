/*
 * Aggregation for individual collections
 * Aggregation for semi-join on fact table 
 * Create individual indexes on inv_date_sk, inv_warehouse_sk, inv_item_sk
 * Create maps on the fly and embed the documents
 * Aggregation for final output of query 
 */

package Query_docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Query21_full_agg {
public static void main(String args[]){
		
		try{
			
			long startTime,endTime,millis,millis1,millis2;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("Dataset_5GB");
					
			DBCollection date = db.getCollection("date_dim");
			DBCollection item = db.getCollection("item");
			DBCollection warehouse = db.getCollection("warehouse");
			DBCollection inventory = db.getCollection("inventory");
			DBCollection q21_partial = db.createCollection("q21_partial_agg", null);
			DBCollection q21_final = db.createCollection("q21_final_agg", null);

			BasicDBList list = new BasicDBList();
			BasicDBObject query = new BasicDBObject();

			HashMap<Object,BasicDBObject> map_d = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_w = new HashMap<Object,BasicDBObject>();
		  
			List<Integer> arr_i = new ArrayList<Integer>();
			List<Integer> arr_d = new ArrayList<Integer>();
			List<Integer> arr_w = new ArrayList<Integer>();
			
			
			//----item aggregation(condition)-----//
						
			DBObject match_i = new BasicDBObject("$match",
					new BasicDBObject("i_current_price",
					new BasicDBObject("$gte",0.99).append("$lte",1.49)));
			
			DBObject groupField_i = new BasicDBObject("_id","$i_current_price");
			groupField_i.put("arr", new BasicDBObject("$addToSet","$i_item_sk"));
			DBObject group_i = new BasicDBObject("$group",groupField_i);
			
			DBObject projectField_i = new BasicDBObject("_id",0);
			projectField_i.put("arr",1);
			DBObject project_i = new BasicDBObject("$project",projectField_i);
			
			List<DBObject> pipeline_i = Arrays.asList(match_i, group_i, project_i);
			AggregationOutput agg_item = item.aggregate(pipeline_i);
			
			for(DBObject val: agg_item.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_i.add((Integer)temp);
			}
			Collections.sort(arr_i);
			list.clear();
			System.out.println(arr_i.size());
			
			//----date aggregation(condition)-----//
			
			
			DBObject match_d = new BasicDBObject("$match",
					new BasicDBObject("d_date",
					new BasicDBObject("$gte","2000-03-20").append("$lte","2000-05-19")));
			
			DBObject groupField_d = new BasicDBObject("_id","$d_year");
			groupField_d.put("arr", new BasicDBObject("$addToSet","$d_date_sk"));
			DBObject group_d = new BasicDBObject("$group",groupField_d);
			
			DBObject projectField_d = new BasicDBObject("_id",0);
			projectField_d.put("arr",1);
			DBObject project_d = new BasicDBObject("$project",projectField_d);
			
			List<DBObject> pipeline_d = Arrays.asList(match_d, group_d, project_d);
			AggregationOutput agg_date = date.aggregate(pipeline_d);
			
			for(DBObject val: agg_date.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_d.add((Integer)temp);
			}
			Collections.sort(arr_d);
			list.clear();
			System.out.println(arr_d.size());
			
			
			//----warehouse aggregation(condition)-----//

			
			DBObject groupField_w = new BasicDBObject("_id","$w_warehouse_sk");
			groupField_w.put("arr", new BasicDBObject("$addToSet","$w_warehouse_sk"));
			DBObject group_w = new BasicDBObject("$group",groupField_w);
			
			DBObject projectField_w = new BasicDBObject("_id",0);
			projectField_w.put("arr",1);
			DBObject project_w = new BasicDBObject("$project",projectField_w);
			
			List<DBObject> pipeline_w = Arrays.asList(group_w, project_w);
			AggregationOutput agg_warehouse = warehouse.aggregate(pipeline_w);
			
			for(DBObject val: agg_warehouse.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_w.add((Integer)temp);
			}
			Collections.sort(arr_w);
			list.clear();
			System.out.println(arr_w.size());
			
			//------query inventory--------------------//
			
			
			list.add(new BasicDBObject("inv_date_sk", new BasicDBObject("$in",arr_d)));
		    list.add(new BasicDBObject("inv_item_sk", new BasicDBObject("$in",arr_i)));
		    list.add(new BasicDBObject("inv_warehouse_sk", new BasicDBObject("$in",arr_w)));
		    query.put("$and", list);
		    System.out.println(query.toString());
		    DBCursor cur_inv = inventory.find(query);
		    int count = 0;
		    
		    q21_partial.createIndex(new BasicDBObject("inv_item_sk",1));
	    	q21_partial.createIndex(new BasicDBObject("inv_date_sk",1));
	    	q21_partial.createIndex(new BasicDBObject("inv_warehouse_sk",1));
	    
		    while(cur_inv.hasNext()){
		    	BasicDBObject val = (BasicDBObject) cur_inv.next();
				count++;
				System.out.println(count);
		    	q21_partial.insert(val);
		    	
		    	
				if(map_i.containsKey(val.get("inv_item_sk"))){
					q21_partial.update(new BasicDBObject("inv_item_sk",val.get("inv_item_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_item_sk",map_i.get(val.get("inv_item_sk")))));		
				}
				else{
					BasicDBObject query_i = new BasicDBObject("i_item_sk",val.get("inv_item_sk"));
					DBCursor cur_i = item.find(query_i);
					while(cur_i.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_i.next();
						doc.removeField("_id");
						doc.removeField("i_item_sk");
						map_i.put(val.get("inv_item_sk"), doc);

						q21_partial.update(new BasicDBObject("inv_item_sk",val.get("inv_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_item_sk",map_i.get(val.get("inv_item_sk")))));				
	
					}
				}
		
				if(map_w.containsKey(val.get("inv_warehouse_sk"))){
					q21_partial.update(new BasicDBObject("inv_warehouse_sk",val.get("inv_warehouse_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_warehouse_sk",map_w.get(val.get("inv_warehouse_sk")))));		
				}
				else{
					BasicDBObject query_w = new BasicDBObject("w_warehouse_sk",val.get("inv_warehouse_sk"));
					DBCursor cur_w = warehouse.find(query_w);
					while(cur_w.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_w.next();
						doc.removeField("_id");
						doc.removeField("w_warehouse_sk");
						map_w.put(val.get("inv_warehouse_sk"), doc);

						q21_partial.update(new BasicDBObject("inv_warehouse_sk",val.get("inv_warehouse_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_warehouse_sk",map_w.get(val.get("inv_warehouse_sk")))));				
	
					}
				}
			
					
				if(map_d.containsKey(val.get("inv_date_sk"))){
					q21_partial.update(new BasicDBObject("inv_date_sk",val.get("inv_date_sk")),
							new BasicDBObject("$set", new BasicDBObject("inv_date_sk",map_d.get(val.get("inv_date_sk")))));		
				}
				else{
					BasicDBObject query_d = new BasicDBObject("d_date_sk",val.get("inv_date_sk"));
					DBCursor cur_d = date.find(query_d);
					while(cur_d.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_d.next();
						doc.removeField("_id");
						doc.removeField("d_date_sk");
						map_d.put(val.get("inv_date_sk"), doc);

						q21_partial.update(new BasicDBObject("inv_date_sk",val.get("inv_date_sk")),
								new BasicDBObject("$set", new BasicDBObject("inv_date_sk",map_d.get(val.get("inv_date_sk")))));				
	
					}
				}
			}
			
			endTime = System.currentTimeMillis();
			millis1 = endTime-startTime;
			//System.out.println("Total time for partial querying (with embedding): "+((millis1/1000)/60)+" minutes "+((millis1 / 1000) % 60)+" seconds");
			System.out.println("Total time for partial querying (with embedding): "+millis1+" milliseconds");
		   
			//-----------final aggregation query on q21_partial------------//
			
			startTime = System.currentTimeMillis();
			
			//--------Grouping---------//
			
			DBObject groupIdKeys = new BasicDBObject( "w_name", "$inv_warehouse_sk.w_warehouse_name");
			groupIdKeys.put("i_id","$inv_item_sk.i_item_id");
			
			/*	inv_before	*/
			List<Object> ltOperands = new ArrayList<Object>();
			ltOperands.add("$inv_date_sk.d_date");
			ltOperands.add("2000-04-19");
			
			List<Object> invBefore_cond = new ArrayList<Object>();
			invBefore_cond.add(new BasicDBObject("$lt",ltOperands));
			invBefore_cond.add("$inv_quantity_on_hand");
			invBefore_cond.add(0);
			BasicDBObject finalinvBefore_cond = new BasicDBObject("$cond",invBefore_cond);
			
			DBObject invBefore = new BasicDBObject("$sum",finalinvBefore_cond);
			
			/*	inv_after	*/
			List<Object> gteOperands = new ArrayList<Object>();
			gteOperands.add("$inv_date_sk.d_date");
			gteOperands.add("2000-04-19");
			
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
			AggregationOutput output = q21_partial.aggregate(pipeline);
			
			//view final collection 
			for(DBObject result : output.results()){
				q21_final.insert(result);
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

