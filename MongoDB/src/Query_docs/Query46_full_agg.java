package Query_docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class Query46_full_agg {
	public static void main(String args[]){
		try{
				long startTime,endTime,millis,millis1,millis2;
				
				startTime = System.currentTimeMillis();
				
				MongoClient mongoClient = new MongoClient("localhost",27017);
				DB db = mongoClient.getDB("Dataset_5GB");
						
				DBCollection store_sales = db.getCollection("store_sales");
				DBCollection house_demo = db.getCollection("household_demographics");
				DBCollection date = db.getCollection("date_dim");
				DBCollection store = db.getCollection("store");
				DBCollection address = db.getCollection("customer_address");
				DBCollection cust_addr = db.getCollection("cust_addr");
				DBCollection q46_partial = db.createCollection("q46_partial_agg", null);
				DBCollection q46_final = db.createCollection("q46_final_agg", null);
				
				BasicDBList list = new BasicDBList();
				BasicDBObject query = new BasicDBObject();

				HashMap<Object,BasicDBObject> map_ca = new HashMap<Object,BasicDBObject>();
				HashMap<Object,BasicDBObject> map_c = new HashMap<Object,BasicDBObject>();
			  
				List<Integer> arr_hd = new ArrayList<Integer>();
				List<Integer> arr_d = new ArrayList<Integer>();
				List<Integer> arr_s = new ArrayList<Integer>();
				List<Integer> arr_ca = new ArrayList<Integer>();
				List<Integer> arr_c = new ArrayList<Integer>();
				
				//----household_demographics aggregation(condition)-----//
				
				List<BasicDBObject> orOperand = new ArrayList<BasicDBObject>();
			    orOperand.add(new BasicDBObject("hd_dep_count",8));
			    orOperand.add(new BasicDBObject("hd_vehicle_count",-1));
			    DBObject match_hd = new BasicDBObject("$match",new BasicDBObject("$or",orOperand));
			   
			    DBObject groupIdKeys_hd = new BasicDBObject( "dep", "$hd_dep_count");
				groupIdKeys_hd.put("event","$hd_vehicle_count");
				
			    DBObject groupField_hd = new BasicDBObject("_id",groupIdKeys_hd);
				groupField_hd.put("arr", new BasicDBObject("$addToSet","$hd_demo_sk"));
				DBObject group_hd = new BasicDBObject("$group",groupField_hd);
				
				DBObject projectField_hd = new BasicDBObject("_id",0);
				projectField_hd.put("arr",1);
				DBObject project_hd = new BasicDBObject("$project",projectField_hd);
				
				List<DBObject> pipeline_hd = Arrays.asList(match_hd, group_hd, project_hd);
				AggregationOutput agg_hd = house_demo.aggregate(pipeline_hd);
				
				for(DBObject val: agg_hd.results()){
					list = (BasicDBList) val.get("arr");
					for(Object temp:list)
						arr_hd.add((Integer)temp);
				}
				Collections.sort(arr_hd);
				list.clear();
				System.out.println(arr_hd.size());
				
				//----date_dim aggregation(condition)-----//
				
				Integer dow_in[] = {0,6};
				Integer year_in[] = {2000,2000+1, 2000+2};
				List<BasicDBObject> andOperand = new ArrayList<BasicDBObject>();
			    andOperand.add(new BasicDBObject("d_dow",new BasicDBObject("$in",dow_in)));
			    andOperand.add(new BasicDBObject("d_year",new BasicDBObject("$in",year_in)));
			    DBObject match_d = new BasicDBObject("$match",new BasicDBObject("$and",andOperand));
			   
			    DBObject groupField_d = new BasicDBObject("_id","$d_year");
				groupField_d.put("arr", new BasicDBObject("$addToSet","$d_date_sk"));
				DBObject group_d = new BasicDBObject("$group",groupField_d);
				
				DBObject projectField_d = new BasicDBObject("_id",0);
				projectField_d.put("arr",1);
				DBObject project_d = new BasicDBObject("$project",projectField_d);
				
				List<DBObject> pipeline_d = Arrays.asList(match_d, group_d, project_d);
				AggregationOutput agg_d = date.aggregate(pipeline_d);
				
				for(DBObject val: agg_d.results()){
					list = (BasicDBList) val.get("arr");
					for(Object temp:list)
						arr_d.add((Integer)temp);
				}
				Collections.sort(arr_d);
				list.clear();
				System.out.println(arr_d.size());
				
				//----store aggregation(condition)-----//
				
				String city_in[] = {"Midway","Fairview","Oak Grove"};
				DBObject matchField_s = new BasicDBObject("s_city",new BasicDBObject("$in",city_in));
				DBObject match_s = new BasicDBObject("$match",matchField_s);
			   
			    DBObject groupField_s = new BasicDBObject("_id","$s_city");
				groupField_s.put("arr", new BasicDBObject("$addToSet","$s_store_sk"));
				DBObject group_s = new BasicDBObject("$group",groupField_s);
				
				DBObject projectField_s = new BasicDBObject("_id",0);
				projectField_s.put("arr",1);
				DBObject project_s = new BasicDBObject("$project",projectField_s);
				
				List<DBObject> pipeline_s = Arrays.asList(match_s, group_s, project_s);
				AggregationOutput agg_s = store.aggregate(pipeline_s);
				
				for(DBObject val: agg_s.results()){
					list = (BasicDBList) val.get("arr");
					for(Object temp:list)
						arr_s.add((Integer)temp);
				}
				Collections.sort(arr_s);
				list.clear();
				System.out.println(arr_s.size());
				
				//----customer_address aggregation(condition)-----//
				
			    DBObject groupField_ca = new BasicDBObject("_id","$ca_address_sk");
				groupField_ca.put("arr", new BasicDBObject("$addToSet","$ca_address_sk"));
				DBObject group_ca = new BasicDBObject("$group",groupField_ca);
				
				DBObject projectField_ca = new BasicDBObject("_id",0);
				projectField_ca.put("arr",1);
				DBObject project_ca = new BasicDBObject("$project",projectField_ca);
				
				List<DBObject> pipeline_ca = Arrays.asList(group_ca, project_ca);
				AggregationOutput agg_ca = address.aggregate(pipeline_ca);
				
				for(DBObject val: agg_ca.results()){
					list = (BasicDBList) val.get("arr");
					for(Object temp:list)
						arr_ca.add((Integer)temp);
				}
				Collections.sort(arr_ca);
				list.clear();
				System.out.println(arr_ca.size());

				//----cust_addr aggregation(condition)-----//
				
			    DBObject groupField_c = new BasicDBObject("_id","$c_customer_sk");
				groupField_c.put("arr", new BasicDBObject("$addToSet","$c_customer_sk"));
				DBObject group_c = new BasicDBObject("$group",groupField_c);
				
				DBObject projectField_c = new BasicDBObject("_id",0);
				projectField_c.put("arr",1);
				DBObject project_c = new BasicDBObject("$project",projectField_c);
				
				List<DBObject> pipeline_c = Arrays.asList(group_c, project_c);
				AggregationOutput agg_c = cust_addr.aggregate(pipeline_c);
				
				for(DBObject val: agg_c.results()){
					list = (BasicDBList) val.get("arr");
					for(Object temp:list)
						arr_c.add((Integer)temp);
				}
				Collections.sort(arr_c);
				list.clear();
				System.out.println(arr_c.size());

				//------store sales aggregation--------------------//
				
				
				list.add(new BasicDBObject("ss_hdemo_sk", new BasicDBObject("$in",arr_hd)));
			    list.add(new BasicDBObject("ss_sold_date_sk", new BasicDBObject("$in",arr_d)));
			    list.add(new BasicDBObject("ss_store_sk", new BasicDBObject("$in",arr_s)));
			    list.add(new BasicDBObject("ss_addr_sk", new BasicDBObject("$in",arr_ca)));
			    list.add(new BasicDBObject("ss_customer_sk", new BasicDBObject("$in",arr_c)));
			    query.put("$and", list);
			    System.out.println(query.toString());
			    DBCursor cur_ss = store_sales.find(query);
			    int count = 0;
			    
			    //q46_partial.createIndex(new BasicDBObject("ss_customer_sk.c_customer_sk",1));
		    	//q46_partial.createIndex(new BasicDBObject("ss_addr_sk.ca_address_sk",1));
			    
			    q46_partial.createIndex(new BasicDBObject("ss_customer_sk",1));
		        q46_partial.createIndex(new BasicDBObject("ss_addr_sk",1));
		        try {
		        	while(cur_ss.hasNext()){
				    	BasicDBObject val = (BasicDBObject) cur_ss.next();
						count++;
						System.out.println(count);
				    	q46_partial.insert(val);
				    	
				    	
						if(map_c.containsKey(val.get("ss_customer_sk"))){
							q46_partial.update(new BasicDBObject("ss_customer_sk",val.get("ss_customer_sk")),
									new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_c.get(val.get("ss_customer_sk")))));		
						}
						else{
							BasicDBObject query_c = new BasicDBObject("c_customer_sk",val.get("ss_customer_sk"));
							DBCursor cur_c = cust_addr.find(query_c);
							while(cur_c.hasNext()){
								BasicDBObject doc = (BasicDBObject) cur_c.next();
								doc.removeField("_id");
								map_c.put(val.get("ss_customer_sk"), doc);

								q46_partial.update(new BasicDBObject("ss_customer_sk",val.get("ss_customer_sk")),
										new BasicDBObject("$set", new BasicDBObject("ss_customer_sk",map_c.get(val.get("ss_customer_sk")))));				
			
							}
						}
				
						if(map_ca.containsKey(val.get("ss_addr_sk"))){
							q46_partial.update(new BasicDBObject("ss_addr_sk",val.get("ss_addr_sk")),
									new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_ca.get(val.get("ss_addr_sk")))));		
						}
						else{
							BasicDBObject query_ca = new BasicDBObject("ca_address_sk",val.get("ss_addr_sk"));
							DBCursor cur_ca = address.find(query_ca);
							while(cur_ca.hasNext()){
								BasicDBObject doc = (BasicDBObject) cur_ca.next();
								doc.removeField("_id");
								map_ca.put(val.get("ss_addr_sk"), doc);

								q46_partial.update(new BasicDBObject("ss_addr_sk",val.get("ss_addr_sk")),
										new BasicDBObject("$set", new BasicDBObject("ss_addr_sk",map_ca.get(val.get("ss_addr_sk")))));				
							}
						}
					}
		        }
		        catch (MongoException e){
		        	System.out.println("Error because of cursor");
		        }
			    				
				endTime = System.currentTimeMillis();
				millis1 = endTime-startTime;
				System.out.println("Total time for partial querying (with embedding): "+((millis1/1000)/60)+" minutes "+((millis1 / 1000) % 60)+" seconds");
			
				//-----------final aggregation query on q46_partial------------//
				
				startTime = System.currentTimeMillis();
				
					
				//----------First projection-----------//
				
				List<Object> neOperand = new ArrayList<Object>();
				neOperand.add("$ss_customer_sk.c_current_addr_sk.ca_city");
				neOperand.add("$ss_addr_sk.ca_city");
				
				DBObject projectField_1 = new BasicDBObject("value",new BasicDBObject("$ne",neOperand));
				projectField_1.put("c_last_name", "$ss_customer_sk.c_last_name");
				projectField_1.put("c_first_name", "$ss_customer_sk.c_first_name");
				projectField_1.put("bought_city", "$ss_addr_sk.ca_city");
				projectField_1.put("ca_city", "$ss_customer_sk.c_current_addr_sk.ca_city");
				projectField_1.put("ss_ticket_number", "$ss_ticket_number");
				projectField_1.put("ss_customer_sk", "$ss_customer_sk.c_customer_sk");
				projectField_1.put("ss_addr_sk", "$ss_addr_sk.ca_address_sk");
				projectField_1.put("amt", "$ss_coupon_amt");
				projectField_1.put("profit", "$ss_net_profit");
				
				DBObject project_1 = new BasicDBObject("$project",projectField_1);
				
				
				//---------Matching-------------------//

				DBObject match = new BasicDBObject("$match",
						new BasicDBObject("value",true));
				
				//--------Grouping---------//
				
				DBObject groupIdKeys = new BasicDBObject( "ss_ticket_number", "$ss_ticket_number");
				groupIdKeys.put("ss_customer_sk","$ss_customer_sk");
				groupIdKeys.put("ss_addr_sk","$ss_addr_sk");
				groupIdKeys.put("ca_city","$ca_city");
				groupIdKeys.put("bought_city","$bought_city");
				groupIdKeys.put("c_last_name","$c_last_name");
				groupIdKeys.put("c_first_name","$c_first_name");
				
				DBObject groupField = new BasicDBObject("_id",groupIdKeys);
				groupField.put("amt", new BasicDBObject("$sum","$amt"));
				groupField.put("profit", new BasicDBObject("$sum","$profit"));
				
				DBObject group = new BasicDBObject("$group", groupField);
				
				//-------Second Projection----------//
				
				DBObject projectField_2 = new BasicDBObject("_id",0);
				projectField_2.put("c_last_name", "$_id.c_last_name");
				projectField_2.put("c_first_name", "$_id.c_first_name");
				projectField_2.put("ca_city", "$_id.ca_city");
				projectField_2.put("bought_city", "$_id.bought_city");
				projectField_2.put("ss_ticket_number", "$_id.ss_ticket_number");
				projectField_2.put("amt",1);
				projectField_2.put("profit",1);
				
				DBObject project_2 = new BasicDBObject("$project",projectField_2);
				
				
				//---------Sorting---------------------//
				
				DBObject sortField = new BasicDBObject("c_last_name",1);
				sortField.put("c_first_name",1);
				sortField.put("ca_city",1);
				sortField.put("bought_city",1);
				sortField.put("ss_ticket_number",1);
				
				DBObject sort = new BasicDBObject("$sort",sortField);
				
		
				//------run aggregation-------------//
				
				List<DBObject> pipeline = Arrays.asList(project_1, match, group, project_2, sort);
				AggregationOutput output = q46_partial.aggregate(pipeline);
				
				//view final collection 
				for(DBObject result : output.results()){
					q46_final.insert(result);
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
