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

public class Query7_full_agg {
	public static void main(String args[]){
		try{
			
			long startTime,endTime,millis,millis1,millis2;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("192.168.2.57",27018);
			DB db = mongoClient.getDB("Dataset_5GB");
					
			DBCollection cust_demo = db.getCollection("customer_demographics");
			DBCollection promotion = db.getCollection("promotion");
			DBCollection date = db.getCollection("date_dim");
			DBCollection item = db.getCollection("item");
			DBCollection store_sales = db.getCollection("store_sales");
			DBCollection q7_partial = db.createCollection("q7_partial_agg", null);
			DBCollection q7_final = db.createCollection("q7_final_agg", null);

			
			BasicDBObject query = new BasicDBObject();
			BasicDBList list = new BasicDBList();

		    //List<BasicDBObject> list = new ArrayList<BasicDBObject>();
			
		    HashMap<Object,BasicDBObject> map_i = new HashMap<Object,BasicDBObject>();
		    List<Integer> arr_cd = new ArrayList<Integer>();
		    List<Integer> arr_p = new ArrayList<Integer>();
		    List<Integer> arr_d = new ArrayList<Integer>();
		    List<Integer> arr_i = new ArrayList<Integer>();
		    
		    //--------customer_demographics aggregation(condition)-----------//
		    
		    List<BasicDBObject> andOperand = new ArrayList<BasicDBObject>();
		    andOperand.add(new BasicDBObject("cd_gender","M"));
		    andOperand.add(new BasicDBObject("cd_education_status","4 yr Degree"));
		    andOperand.add(new BasicDBObject("cd_marital_status","M"));
		    DBObject match_cd = new BasicDBObject("$match",new BasicDBObject("$and",andOperand));
		    
		    DBObject groupField_cd = new BasicDBObject("_id","$cd_gender");
			groupField_cd.put("arr", new BasicDBObject("$addToSet","$cd_demo_sk"));
			DBObject group_cd = new BasicDBObject("$group",groupField_cd);
			
			DBObject projectField_cd = new BasicDBObject("_id",0);
			projectField_cd.put("arr",1);
			DBObject project_cd = new BasicDBObject("$project",projectField_cd);
			
			List<DBObject> pipeline_cd = Arrays.asList(match_cd, group_cd, project_cd);
			AggregationOutput agg_custdemo = cust_demo.aggregate(pipeline_cd);
			
			for(DBObject val: agg_custdemo.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_cd.add((Integer)temp);
			}
			Collections.sort(arr_cd);
			list.clear();
			System.out.println(arr_cd.size());
			
			//--------------promotion aggregation(condition)--------------//
			
			List<BasicDBObject> orOperand = new ArrayList<BasicDBObject>();
		    orOperand.add(new BasicDBObject("p_channel_email","N"));
		    orOperand.add(new BasicDBObject("p_channel_event","N"));
		    DBObject match_p = new BasicDBObject("$match",new BasicDBObject("$or",orOperand));
		    
		    DBObject groupIdKeys_p = new BasicDBObject( "email", "$p_channel_email");
			groupIdKeys_p.put("event","$p_channel_event");
			
		    DBObject groupField_p = new BasicDBObject("_id",groupIdKeys_p);
			groupField_p.put("arr", new BasicDBObject("$addToSet","$p_promo_sk"));
			DBObject group_p = new BasicDBObject("$group",groupField_p);
			
			DBObject projectField_p = new BasicDBObject("_id",0);
			projectField_p.put("arr",1);
			DBObject project_p = new BasicDBObject("$project",projectField_p);
			
			List<DBObject> pipeline_p = Arrays.asList(match_p, group_p, project_p);
			AggregationOutput agg_p = promotion.aggregate(pipeline_p);
			
			for(DBObject val: agg_p.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_p.add((Integer)temp);
			}
			Collections.sort(arr_p);
			list.clear();
			System.out.println(arr_p.size());
			
			//----------------date_dim aggregation(condition)-------------//
			
			DBObject match_d = new BasicDBObject("$match", new BasicDBObject("d_year",2001));
			
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
			
			
			//----------------item aggregation(condition)-------------//
			
			DBObject groupField_i = new BasicDBObject("_id","$i_item_id");
			groupField_i.put("arr", new BasicDBObject("$addToSet","$i_item_sk"));
			DBObject group_i = new BasicDBObject("$group",groupField_i);
			
			DBObject projectField_i = new BasicDBObject("_id",0);
			projectField_i.put("arr",1);
			DBObject project_i = new BasicDBObject("$project",projectField_i);
			
			List<DBObject> pipeline_i = Arrays.asList(group_i, project_i);
			AggregationOutput agg_i = item.aggregate(pipeline_i);
			
			for(DBObject val: agg_i.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_i.add((Integer)temp);
			}
			Collections.sort(arr_i);
			list.clear();
			System.out.println(arr_i.size());
			
			
			//---------------query store_sales semi-join--------------------//
			list.add(new BasicDBObject("ss_cdemo_sk", new BasicDBObject("$in",arr_cd)));
		    list.add(new BasicDBObject("ss_promo_sk", new BasicDBObject("$in",arr_p)));
		    list.add(new BasicDBObject("ss_sold_date_sk", new BasicDBObject("$in",arr_d)));
		    list.add(new BasicDBObject("ss_item_sk", new BasicDBObject("$in",arr_i)));
		    query.put("$and", list);
		    //System.out.println(query.toString());
		    DBCursor cur_ss = store_sales.find(query);
		    int count = 0;
		    
	    	q7_partial.createIndex(new BasicDBObject("ss_item_sk",1));

		    while(cur_ss.hasNext()){
				BasicDBObject temp2 = (BasicDBObject) cur_ss.next();
				q7_partial.insert(temp2);
		    	
		    	//-----embed the item docs on the fly----//
		    	if(map_i.containsKey(temp2.get("ss_item_sk"))){
					q7_partial.update(new BasicDBObject("ss_item_sk",temp2.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map_i.get(temp2.get("ss_item_sk")))));					
				}
		    	else{
		    		BasicDBObject query_i = new BasicDBObject("i_item_sk",temp2.get("ss_item_sk"));
					DBCursor cur_i = item.find(query_i);
					while(cur_i.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_i.next();
						doc.removeField("_id");
						doc.removeField("i_item_sk");
						map_i.put(temp2.get("ss_item_sk"), doc);

						q7_partial.update(new BasicDBObject("ss_item_sk",temp2.get("ss_item_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_item_sk",map_i.get(temp2.get("ss_item_sk")))));				
	
					}
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
		    
			//-----------final aggregation query on q7_partial------------//
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
			AggregationOutput output = q7_partial.aggregate(pipeline);
			
			//view final collection 
			for(DBObject result : output.results()){
				q7_final.insert(result);
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
