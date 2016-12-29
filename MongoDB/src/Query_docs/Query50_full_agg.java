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

public class Query50_full_agg {
public static void main(String args[]){
		
		try{
			
			long startTime,endTime,millis,millis1,millis2,millis3;
			
			startTime = System.currentTimeMillis();
			
			MongoClient mongoClient = new MongoClient("localhost",27017);
			DB db = mongoClient.getDB("Dataset_5GB");
					
			DBCollection date = db.getCollection("date_dim");
			DBCollection store = db.getCollection("store");
			DBCollection store_sales = db.getCollection("store_sales");
			DBCollection store_returns = db.getCollection("store_returns");
			DBCollection q50_ss_partial = db.createCollection("q50_ss_partial_agg", null);
			DBCollection q50_sr_partial = db.createCollection("q50_sr_partial_agg", null);
			DBCollection q50_final = db.createCollection("q50_final_agg", null);

			BasicDBList list = new BasicDBList();

			HashMap<Object,BasicDBObject> map_sr = new HashMap<Object,BasicDBObject>();
			HashMap<Object,BasicDBObject> map_s = new HashMap<Object,BasicDBObject>();
		  
			List<Object> arr_d1 = new ArrayList<Object>();
			List<Object> arr_d2 = new ArrayList<Object>();
			List<Object> arr_s = new ArrayList<Object>();
			List<Object> sr_keys = new ArrayList<Object>();
			
			//****************store_returns**************//
			//----date aggregation(condition)-----//
						
			List<BasicDBObject> andOperand_d2 = new ArrayList<BasicDBObject>();
			andOperand_d2.add(new BasicDBObject("d_year",1998));
			andOperand_d2.add(new BasicDBObject("d_moy",10));
			DBObject and_d2 = new BasicDBObject("$and",andOperand_d2);
			DBObject match_d2 = new BasicDBObject("$match",and_d2);
					
			DBObject groupField_d2 = new BasicDBObject("_id","$d_year");
			groupField_d2.put("arr", new BasicDBObject("$addToSet","$d_date_sk"));
			DBObject group_d2 = new BasicDBObject("$group",groupField_d2);
			
			DBObject projectField_d2 = new BasicDBObject("_id",0);
			projectField_d2.put("arr",1);
			DBObject project_d2 = new BasicDBObject("$project",projectField_d2);
			
			List<DBObject> pipeline_d2 = Arrays.asList(match_d2, group_d2, project_d2);
			AggregationOutput agg_date2 = date.aggregate(pipeline_d2);
			
			for(DBObject val: agg_date2.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_d2.add(temp);
			}
			list.clear();
			System.out.println(arr_d2.size());
			
			//----store_returns aggregation------//
			
			q50_sr_partial.createIndex(new BasicDBObject("sr_keys",1));
			
			DBObject match_sr = new BasicDBObject("$match", new BasicDBObject("sr_returned_date_sk",new BasicDBObject("$in",arr_d2)));
			
			List<Object> substr1_sr = new ArrayList<Object>();
			substr1_sr.add("$sr_item_sk");
			substr1_sr.add(0);
			substr1_sr.add(10);
			List<Object> substr2_sr = new ArrayList<Object>();
			substr2_sr.add("$sr_ticket_number");
			substr2_sr.add(0);
			substr2_sr.add(10);
			List<Object> substr3_sr = new ArrayList<Object>();
			substr3_sr.add("$sr_customer_sk");
			substr3_sr.add(0);
			substr3_sr.add(10);
			
			List<Object> concatlist_sr = new ArrayList<Object>();
			concatlist_sr.add(new BasicDBObject("$substr",substr1_sr));
			concatlist_sr.add(" ");
			concatlist_sr.add(new BasicDBObject("$substr",substr2_sr));
			concatlist_sr.add(" ");
			concatlist_sr.add(new BasicDBObject("$substr",substr3_sr));
			
			DBObject projectField_sr = new BasicDBObject("sr_keys",new BasicDBObject("$concat",concatlist_sr));
			projectField_sr.put("sr_returned_date_sk", "$sr_returned_date_sk");
			projectField_sr.put("sr_return_time_sk","$sr_return_time_sk");
			projectField_sr.put("sr_item_sk","$sr_item_sk");
			projectField_sr.put("sr_customer_sk","$sr_customer_sk");
			projectField_sr.put("sr_cdemo_sk","$sr_cdemo_sk");
			projectField_sr.put("sr_hdemo_sk","$sr_hdemo_sk");
			projectField_sr.put("sr_addr_sk","$sr_addr_sk");
			projectField_sr.put("sr_store_sk","$sr_store_sk");
			projectField_sr.put("sr_reason_sk","$sr_reason_sk");
			projectField_sr.put("sr_ticket_number","$sr_ticket_number");
			projectField_sr.put("sr_return_quantity","$sr_return_quantity");
			projectField_sr.put("sr_return_amt","$sr_return_amt");
			projectField_sr.put("sr_return_tax","$sr_return_tax");
			projectField_sr.put("sr_return_amt_inc_tax","$sr_return_amt_inc_tax");
			projectField_sr.put("sr_fee","$sr_fee");
			projectField_sr.put("sr_return_ship_cost","$sr_return_ship_cost");
			projectField_sr.put("sr_refunded_cash","$sr_refunded_cash");
			projectField_sr.put("sr_reversed_charge","$sr_reversed_charge");
			projectField_sr.put("sr_store_credit","$sr_store_credit");
			projectField_sr.put("sr_net_loss","$sr_net_loss");
			
			DBObject project_sr = new BasicDBObject("$project",projectField_sr);
			
			List<DBObject> pipeline_sr = Arrays.asList(match_sr, project_sr);
			AggregationOutput agg_sr = store_returns.aggregate(pipeline_sr);
			
			for(DBObject val: agg_sr.results()){
				q50_sr_partial.insert(val);
			}
			
			//---------aggregate q50_sr-------------//
			DBObject groupField_q50sr = new BasicDBObject("_id","$sr_keys");
			groupField_q50sr.put("arr", new BasicDBObject("$addToSet","$sr_keys"));
			DBObject group_q50sr = new BasicDBObject("$group",groupField_q50sr);
			
			DBObject projectField_q50sr = new BasicDBObject("_id",0);
			projectField_q50sr.put("arr", 1);
			DBObject project_q50sr = new BasicDBObject("$project",projectField_q50sr);
			
			List<DBObject> pipeline_q50sr = Arrays.asList(group_q50sr, project_q50sr);
			AggregationOutput agg_q50sr = q50_sr_partial.aggregate(pipeline_q50sr);
			for(DBObject val: agg_q50sr.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					sr_keys.add(temp);
			}
			list.clear();
			System.out.println(sr_keys.size());
			
			
			//**********store_sales aggregation*************//
			
			DBObject groupField_s = new BasicDBObject("_id","$s_store_sk");
			groupField_s.put("arr", new BasicDBObject("$addToSet","$s_store_sk"));
			DBObject group_s = new BasicDBObject("$group",groupField_s);
			
			DBObject projectField_s = new BasicDBObject("_id",0);
			projectField_s.put("arr",1);
			DBObject project_s = new BasicDBObject("$project",projectField_s);
			
			List<DBObject> pipeline_s = Arrays.asList(group_s, project_s);
			AggregationOutput agg_store = store.aggregate(pipeline_s);
			
			for(DBObject val: agg_store.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_s.add(temp);
			}
			list.clear();
			System.out.println(arr_s.size());
			
			//----------------------//
			
			DBObject groupField_d1 = new BasicDBObject("_id","$d_day_name");
			groupField_d1.put("arr", new BasicDBObject("$addToSet","$d_date_sk"));
			DBObject group_d1 = new BasicDBObject("$group",groupField_d1);
			
			DBObject projectField_d1 = new BasicDBObject("_id",0);
			projectField_d1.put("arr",1);
			DBObject project_d1 = new BasicDBObject("$project",projectField_d1);
			
			List<DBObject> pipeline_d1 = Arrays.asList(group_d1, project_d1);
			AggregationOutput agg_date1 = date.aggregate(pipeline_d1);
			
			for(DBObject val: agg_date1.results()){
				list = (BasicDBList) val.get("arr");
				for(Object temp:list)
					arr_d1.add(temp);
			}
			list.clear();
			System.out.println(arr_d1.size());
			
			//----final aggregation-----//
			
			
			System.out.println("Starting....1");
			
			List<Object> substr1_ss = new ArrayList<Object>();
			substr1_ss.add("$ss_item_sk");
			substr1_ss.add(0);
			substr1_ss.add(10);
			List<Object> substr2_ss = new ArrayList<Object>();
			substr2_ss.add("$ss_ticket_number");
			substr2_ss.add(0);
			substr2_ss.add(10);
			List<Object> substr3_ss = new ArrayList<Object>();
			substr3_ss.add("$ss_customer_sk");
			substr3_ss.add(0);
			substr3_ss.add(10);
			
			System.out.println("Starting....2");
			List<Object> concatlist_ss = new ArrayList<Object>();
			concatlist_ss.add(new BasicDBObject("$substr",substr1_ss));
			concatlist_ss.add(" ");
			concatlist_ss.add(new BasicDBObject("$substr",substr2_ss));
			concatlist_ss.add(" ");
			concatlist_ss.add(new BasicDBObject("$substr",substr3_ss));
			
			System.out.println("Starting....3");
			DBObject projectField_ss = new BasicDBObject("ss_keys",new BasicDBObject("$concat",concatlist_ss));
			projectField_ss.put("ss_sold_date_sk", "$ss_sold_date_sk");
			projectField_ss.put("ss_sold_time_sk","$ss_sold_time_sk");
			projectField_ss.put("ss_item_sk","$ss_item_sk");
			projectField_ss.put("ss_customer_sk","$ss_customer_sk");
			projectField_ss.put("ss_cdemo_sk","$ss_cdemo_sk");
			projectField_ss.put("ss_hdemo_sk","$ss_hdemo_sk");
			projectField_ss.put("ss_addr_sk","$ss_addr_sk");
			projectField_ss.put("ss_store_sk","$ss_store_sk");
			projectField_ss.put("ss_promo_sk","$sr_promo_sk");
			projectField_ss.put("ss_ticket_number","$ss_ticket_number");
			projectField_ss.put("ss_quantity","$ss_quantity");
			projectField_ss.put("ss_wholesale_cost","$ss_wholesale_cost");
			projectField_ss.put("ss_list_price","$ss_list_price");
			projectField_ss.put("ss_sales_price","$ss_sales_price");
			projectField_ss.put("ss_ext_discount_amt","$ss_ext_discount_amt");
			projectField_ss.put("ss_ext_sales_price","$ss_ext_sales_price");
			projectField_ss.put("ss_ext_wholesale_cost","$ss_ext_wholesale_cost");
			projectField_ss.put("ss_ext_list_price","$ss_ext_list_price");
			projectField_ss.put("ss_ext_tax","$ss_ext_tax");
			projectField_ss.put("ss_coupon_amt","$ss_coupon_amt");
			projectField_ss.put("ss_net_paid","$ss_net_paid");
			projectField_ss.put("ss_net_paid_inc_tax","$ss_net_paid_inc_tax");
			projectField_ss.put("ss_net_profit","$ss_net_profit");
			DBObject project_ss = new BasicDBObject("$project",projectField_ss);
			
			System.out.println("Starting....4");
			List<BasicDBObject> andOperand_ss = new ArrayList<BasicDBObject>();
			andOperand_ss.add(new BasicDBObject("ss_keys",new BasicDBObject("$in",sr_keys)));
			andOperand_ss.add(new BasicDBObject("ss_sold_date_sk",new BasicDBObject("$in",arr_d1)));
			andOperand_ss.add(new BasicDBObject("ss_store_sk",new BasicDBObject("$in",arr_s)));
			DBObject match_ss = new BasicDBObject("$match",new BasicDBObject("$and",andOperand_ss));
			
			System.out.println("Starting....5");
			List<DBObject> pipeline_ss = Arrays.asList(project_ss,match_ss);
			AggregationOutput agg_ss = store_sales.aggregate(pipeline_ss);
			System.out.println("Starting....6");
			
			for(DBObject val: agg_ss.results()){
				q50_ss_partial.insert(val);
			}
			
			endTime = System.currentTimeMillis();
			millis1 = endTime-startTime;
			//System.out.println("Total time for aggregation: "+((millis2/1000)/60)+" minutes "+((millis2 / 1000) % 60)+" seconds");
			System.out.println("Total time for partial aggregation: "+millis1+" milliseconds");
			
			//****************************************************************************//
			//****************************************************************************//
			startTime = System.currentTimeMillis();
			
			//q50_sr_partial.createIndex(new BasicDBObject("sr_keys",1));
			q50_ss_partial.createIndex(new BasicDBObject("ss_keys",1));
			q50_ss_partial.createIndex(new BasicDBObject("ss_store_sk",1));
			
			DBCursor cur_ss = q50_ss_partial.find();
			int count=0;
			while(cur_ss.hasNext()){
				BasicDBObject val = (BasicDBObject) cur_ss.next();
				count++;
				System.out.println(count);
				
				if(map_sr.containsKey(val.get("ss_keys"))){
					q50_ss_partial.update(new BasicDBObject("ss_keys",val.get("ss_keys")),
							new BasicDBObject("$set", new BasicDBObject("ss_keys",map_sr.get(val.get("ss_keys")))));		
				}
				else{
					BasicDBObject query_sr = new BasicDBObject("sr_keys",val.get("ss_keys"));
					DBCursor cur_sr = q50_sr_partial.find(query_sr);
					while(cur_sr.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_sr.next();
						doc.removeField("_id");
						map_sr.put(val.get("ss_keys"), doc);

						q50_ss_partial.update(new BasicDBObject("ss_keys",val.get("ss_keys")),
								new BasicDBObject("$set", new BasicDBObject("ss_keys",map_sr.get(val.get("ss_keys")))));				
	
					}
				}
				
				if(map_s.containsKey(val.get("ss_store_sk"))){
					q50_ss_partial.update(new BasicDBObject("ss_store_sk",val.get("ss_store_sk")),
							new BasicDBObject("$set", new BasicDBObject("ss_store_sk",map_s.get(val.get("ss_store_sk")))));		
				}
				else{
					BasicDBObject query_s = new BasicDBObject("s_store_sk",val.get("ss_store_sk"));
					DBCursor cur_s = store.find(query_s);
					while(cur_s.hasNext()){
						BasicDBObject doc = (BasicDBObject) cur_s.next();
						doc.removeField("_id");
						map_s.put(val.get("ss_store_sk"), doc);

						q50_ss_partial.update(new BasicDBObject("ss_store_sk",val.get("ss_store_sk")),
								new BasicDBObject("$set", new BasicDBObject("ss_store_sk",map_s.get(val.get("ss_store_sk")))));				
	
					}
				}
			}
					
			endTime = System.currentTimeMillis();
			millis2 = endTime-startTime;
			//System.out.println("Total time for aggregation: "+((millis2/1000)/60)+" minutes "+((millis2 / 1000) % 60)+" seconds");
			System.out.println("Total time for embedding: "+millis2+" milliseconds");
			
			//****************************************************************************//
			//****************************************************************************//
			startTime = System.currentTimeMillis();

			DBObject groupIdkeys = new BasicDBObject("store","$ss_store_sk.s_store_name");
			groupIdkeys.put("company","$ss_store_sk.s_company_id");
			groupIdkeys.put("str_num","$ss_store_sk.s_street_number");
			groupIdkeys.put("str_name","$ss_store_sk.s_street_name");
			groupIdkeys.put("str_type","$ss_store_sk.s_street_type");
			groupIdkeys.put("suite_num","$ss_store_sk.s_suite_number");
			groupIdkeys.put("city","$ss_store_sk.s_city");
			groupIdkeys.put("county","$ss_store_sk.s_county");
			groupIdkeys.put("state","$ss_store_sk.s_state");
			groupIdkeys.put("zip","$ss_store_sk.s_zip");
		    //-------//	   
		    List<Object> subOperand_30 = new ArrayList<Object>();
		    subOperand_30.add("$ss_keys.sr_returned_date_sk");
		    subOperand_30.add("$ss_sold_date_sk");
		    DBObject subtract_30 = new BasicDBObject("$subtract",subOperand_30);
		    
		    List<Object> lteOperand_30 = new ArrayList<Object>();
		    lteOperand_30.add(subtract_30);
		    lteOperand_30.add(30);
		    DBObject lte_30 = new BasicDBObject("$lte",lteOperand_30);
		    
		    List<Object> condOperand_30 = new ArrayList<Object>();
		    condOperand_30.add(lte_30);
		    condOperand_30.add(1);
		    condOperand_30.add(0);
		    DBObject cond_30 = new BasicDBObject("$cond",condOperand_30);
		    
		    //------//
		    List<Object> subOperand_31 = new ArrayList<Object>();
		    subOperand_31.add("$ss_keys.sr_returned_date_sk");
		    subOperand_31.add("$ss_sold_date_sk");
		    DBObject subtract_31 = new BasicDBObject("$subtract",subOperand_31);
		    
		    List<Object> gtOperand_31 = new ArrayList<Object>();
		    gtOperand_31.add(subtract_31);
		    gtOperand_31.add(30);
		    DBObject gt_31 = new BasicDBObject("$gt",gtOperand_31);
		    
		    List<Object> subOperand_60 = new ArrayList<Object>();
		    subOperand_60.add("$ss_keys.sr_returned_date_sk");
		    subOperand_60.add("$ss_sold_date_sk");
		    DBObject subtract_60 = new BasicDBObject("$subtract",subOperand_60);
		    
		    List<Object> lteOperand_60 = new ArrayList<Object>();
		    lteOperand_60.add(subtract_60);
		    lteOperand_60.add(60);
		    DBObject lte_60 = new BasicDBObject("$lte",lteOperand_60);
		    
		    List<Object> andOperand_31 = new ArrayList<Object>();
		    andOperand_31.add(gt_31);
		    andOperand_31.add(lte_60);
		    DBObject and_31 = new BasicDBObject("$and",andOperand_31);
		    
		    List<Object> condOperand_31 = new ArrayList<Object>();
		    condOperand_31.add(and_31);
		    condOperand_31.add(1);
		    condOperand_31.add(0);
		    DBObject cond_31 = new BasicDBObject("$cond",condOperand_31);
		    
		    //------------//
		    List<Object> subOperand_61 = new ArrayList<Object>();
		    subOperand_61.add("$ss_keys.sr_returned_date_sk");
		    subOperand_61.add("$ss_sold_date_sk");
		    DBObject subtract_61 = new BasicDBObject("$subtract",subOperand_61);
		    
		    List<Object> gtOperand_61 = new ArrayList<Object>();
		    gtOperand_61.add(subtract_61);
		    gtOperand_61.add(60);
		    DBObject gt_61 = new BasicDBObject("$gt",gtOperand_61);
		    
		    List<Object> subOperand_90 = new ArrayList<Object>();
		    subOperand_90.add("$ss_keys.sr_returned_date_sk");
		    subOperand_90.add("$ss_sold_date_sk");
		    DBObject subtract_90 = new BasicDBObject("$subtract",subOperand_90);
		    
		    List<Object> lteOperand_90 = new ArrayList<Object>();
		    lteOperand_90.add(subtract_90);
		    lteOperand_90.add(90);
		    DBObject lte_90 = new BasicDBObject("$lte",lteOperand_90);
		    
		    List<Object> andOperand_61 = new ArrayList<Object>();
		    andOperand_61.add(gt_61);
		    andOperand_61.add(lte_90);
		    DBObject and_61 = new BasicDBObject("$and",andOperand_61);
		    
		    List<Object> condOperand_61 = new ArrayList<Object>();
		    condOperand_61.add(and_61);
		    condOperand_61.add(1);
		    condOperand_61.add(0);
		    DBObject cond_61 = new BasicDBObject("$cond",condOperand_61);
		    
		    //-----------//
		    List<Object> subOperand_91 = new ArrayList<Object>();
		    subOperand_91.add("$ss_keys.sr_returned_date_sk");
		    subOperand_91.add("$ss_sold_date_sk");
		    DBObject subtract_91 = new BasicDBObject("$subtract",subOperand_91);
		    
		    List<Object> gtOperand_91 = new ArrayList<Object>();
		    gtOperand_91.add(subtract_91);
		    gtOperand_91.add(90);
		    DBObject gt_91 = new BasicDBObject("$gt",gtOperand_91);
		    
		    List<Object> subOperand_120 = new ArrayList<Object>();
		    subOperand_120.add("$ss_keys.sr_returned_date_sk");
		    subOperand_120.add("$ss_sold_date_sk");
		    DBObject subtract_120 = new BasicDBObject("$subtract",subOperand_120);
		    
		    List<Object> lteOperand_120 = new ArrayList<Object>();
		    lteOperand_120.add(subtract_120);
		    lteOperand_120.add(120);
		    DBObject lte_120 = new BasicDBObject("$lte",lteOperand_120);
		    
		    List<Object> andOperand_91 = new ArrayList<Object>();
		    andOperand_91.add(gt_91);
		    andOperand_91.add(lte_120);
		    DBObject and_91 = new BasicDBObject("$and",andOperand_91);
		    
		    List<Object> condOperand_91 = new ArrayList<Object>();
		    condOperand_91.add(and_91);
		    condOperand_91.add(1);
		    condOperand_91.add(0);
		    DBObject cond_91 = new BasicDBObject("$cond",condOperand_91);
		    
		    //--------//
		    List<Object> subOperand_121 = new ArrayList<Object>();
		    subOperand_121.add("$ss_keys.sr_returned_date_sk");
		    subOperand_121.add("$ss_sold_date_sk");
		    DBObject subtract_121 = new BasicDBObject("$subtract",subOperand_121);
		    
		    List<Object> gtOperand_121 = new ArrayList<Object>();
		    gtOperand_121.add(subtract_121);
		    gtOperand_121.add(120);
		    DBObject gt_121 = new BasicDBObject("$gt",gtOperand_121);
		    
		    List<Object> condOperand_121 = new ArrayList<Object>();
		    condOperand_121.add(gt_121);
		    condOperand_121.add(1);
		    condOperand_121.add(0);
		    DBObject cond_121 = new BasicDBObject("$cond",condOperand_121);
		    //------//
		    
		    DBObject groupField = new BasicDBObject("_id",groupIdkeys);
		    groupField.put("30 days", new BasicDBObject("$sum",cond_30));
		    groupField.put("31-60 days", new BasicDBObject("$sum",cond_31));
		    groupField.put("61-90 days", new BasicDBObject("$sum",cond_61));
		    groupField.put("91-120 days", new BasicDBObject("$sum",cond_91));
		    groupField.put(">120 days", new BasicDBObject("$sum",cond_121));
		    DBObject group = new BasicDBObject("$group",groupField);
		    
		    DBObject projectField = new BasicDBObject("_id",0);
		    projectField.put("s_store_name", "$_id.store");
		    projectField.put("s_company_id", "$_id.company");
		    projectField.put("s_street_number", "$_id.str_num");
		    projectField.put("s_street_name", "$_id.str_name");
		    projectField.put("s_street_type", "$_id.str_type");
		    projectField.put("s_suite_number", "$_id.suite_num");
		    projectField.put("s_city", "$_id.city");
		    projectField.put("s_county", "$_id.county");
		    projectField.put("s_state", "$_id.state");
		    projectField.put("s_zip", "$_id.zip");
		    projectField.put("30 days",1);
		    projectField.put("31-60 days",1);
		    projectField.put("61-90 days",1);
		    projectField.put("91-120 days",1);
		    projectField.put(">120 days",1);
		    DBObject project = new BasicDBObject("$project",projectField);
		    
		    DBObject sortField = new BasicDBObject("s_store_name",1);
		    sortField.put("s_company_id", 1);
		    sortField.put("s_street_number", 1);
		    sortField.put("s_street_name", 1);
		    sortField.put("s_street_type", 1);
		    sortField.put("s_suite_number", 1);
		    sortField.put("s_city", 1);
		    sortField.put("s_county", 1);
		    sortField.put("s_state", 1);
		    sortField.put("s_zip", 1);
		    DBObject sort = new BasicDBObject("$sort",sortField);
		    
		    
			List<DBObject> pipeline = Arrays.asList(group, project, sort);
			AggregationOutput output = q50_ss_partial.aggregate(pipeline);
			
			//view final collection 
			for(DBObject result : output.results()){
				q50_final.insert(result);
			}
			
			
			endTime = System.currentTimeMillis();
			millis3 = endTime-startTime;
			//System.out.println("Total time for aggregation: "+((millis2/1000)/60)+" minutes "+((millis2 / 1000) % 60)+" seconds");
			System.out.println("Total time for full aggregation: "+millis3+" milliseconds");
			
			millis = millis1 + millis2 + millis3;
			//System.out.println("Total time to run query: "+((millis/1000)/60)+" minutes "+((millis / 1000) % 60)+" seconds");
			System.out.println("Total time to run query: "+millis+" milliseconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}	
}

