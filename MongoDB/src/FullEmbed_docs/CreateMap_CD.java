package FullEmbed_docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CreateMap_CD {

	public CreateMap_CD(DB db_, HashMap<Object,BasicDBObject> map_cd){
		
		DB db = db_;
		
		//--finding distinct ss_cdemo_sk in store_sales-----//
		DBCollection store_sales = db.getCollection("store_sales_embed");
		
		List<Object> list = new ArrayList<Object>();
		
		DBObject groupField = new BasicDBObject( "_id", "$ss_cdemo_sk");
		DBObject group = new BasicDBObject("$group", groupField);

		List<DBObject> pipeline = Arrays.asList(group);
		AggregationOutput output = store_sales.aggregate(pipeline);
		
		//view final collection 
		for(DBObject result : output.results()){
			list.add(result.get("_id"));
		}
		/*
		System.out.println(list.size());
		for(int i =0;i <10;i++)
			System.out.println(list.get(i));
		*/
		//--------------------------------------------------//
		
		
		DBCollection cust_demo = db.getCollection("customer_demographics");
		map_cd = new HashMap<Object,BasicDBObject>();

		
		BasicDBObject query = new BasicDBObject();
		query.put("cd_demo_sk", new BasicDBObject("$in",list));
		//System.out.println(query.toString());
		DBCursor cur_cd = cust_demo.find(query);
		int count=0;
		while(cur_cd.hasNext()){
			count++;
			BasicDBObject val = (BasicDBObject) cur_cd.next();
			BasicDBObject doc = (BasicDBObject) val.copy();
			doc.removeField("_id");
			//doc.put("_id", new ObjectId());
			doc.removeField("cd_demo_sk");
			map_cd.put(val.get("cd_demo_sk"), doc);
		}
		System.out.println("Total maps in customer_demographics :"+count);
	}
}
