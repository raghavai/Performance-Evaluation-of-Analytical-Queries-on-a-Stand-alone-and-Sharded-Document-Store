package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;
import com.mongodb.MongoClient;


public class Item {
	public Item(DB db){
	
		try{
			
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/item.dat");
			DBCollection collection = db.createCollection("item", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"i_item_sk");map.put(1,"i_item_id");map.put(2,"i_rec_start_date");
			map.put(3,"i_rec_end_date");map.put(4,"i_item_desc");map.put(5,"i_current_price");
			map.put(6,"i_wholesale_cost");map.put(7,"i_brand_id");map.put(8,"i_brand");
			map.put(9,"i_class_id");map.put(10,"i_class");map.put(11,"i_category_id");
			map.put(12,"i_category");map.put(13,"i_manufact_id");map.put(14,"i_manufact");
			map.put(15,"i_size");map.put(16,"i_formulation");map.put(17,"i_color");
			map.put(18,"i_units");map.put(19,"i_container");map.put(20,"i_manager_id");
			map.put(21,"i_product_name");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load item data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
