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

public class Store {
	public Store(DB db){
		
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/store.dat");
			DBCollection collection = db.createCollection("store", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"s_store_sk");map.put(1,"s_store_id");map.put(2,"s_rec_start_date");
			map.put(3,"s_rec_end_date");map.put(4,"s_closed_date_sk");map.put(5,"s_store_name");
			map.put(6,"s_number_employees");map.put(7,"s_floor_space");map.put(8,"s_hours");
			map.put(9,"s_manager");map.put(10,"s_market_id");map.put(11,"s_geography_class");
			map.put(12,"s_market_desc");map.put(13,"s_market_manager");map.put(14,"s_division_id");
			map.put(15,"s_division_name");map.put(16,"s_company_id");map.put(17,"s_company_name");
			map.put(18,"s_street_number");map.put(19,"s_street_name");map.put(20,"s_street_type");
			map.put(21,"s_suite_number");map.put(22,"s_city");map.put(23,"s_county");
			map.put(24,"s_state");map.put(25,"s_zip");map.put(26,"s_country");
			map.put(27,"s_gm_offset");map.put(28,"s_tax_percentage");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load store data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	
	}
}
