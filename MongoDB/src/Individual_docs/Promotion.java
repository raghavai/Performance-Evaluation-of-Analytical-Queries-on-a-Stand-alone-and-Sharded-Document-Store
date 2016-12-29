package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;
import com.mongodb.MongoClient;


public class Promotion {
	public Promotion(DB db){
	
		try{
			
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/promotion.dat");
			DBCollection collection = db.createCollection("promotion", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			

			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"p_promo_sk");map.put(1,"p_promo_id");map.put(2,"p_start_date_sk");
			map.put(3,"p_end_date_sk");map.put(4,"p_item_sk");map.put(5,"p_cost");
			map.put(6,"p_response_target");map.put(7,"p_promo_name");map.put(8,"p_channel_dmail");
			map.put(9,"p_channel_email");map.put(10,"p_channel_catalog");map.put(11,"p_channel_tv");
			map.put(12,"p_channel_radio");map.put(13,"p_channel_press");map.put(14,"p_channel_event");
			map.put(15,"p_channel_demo");map.put(16,"p_channel_details");map.put(17,"p_purpose");
			map.put(18,"p_discount_active");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load promotion data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
