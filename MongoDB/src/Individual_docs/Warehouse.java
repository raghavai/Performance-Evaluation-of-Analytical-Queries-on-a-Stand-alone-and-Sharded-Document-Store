package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;


public class Warehouse{
	public Warehouse(DB db){
		
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/warehouse.dat");
			DBCollection collection = db.createCollection("warehouse", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"w_warehouse_sk");map.put(1,"w_warehouse_id");map.put(2,"w_warehouse_name");
			map.put(3,"w_warehouse_sq_ft");map.put(4,"w_street_number");map.put(5,"w_street_name");
			map.put(6,"w_street_type");map.put(7,"w_suite_number");map.put(8,"w_city");
			map.put(9,"w_county");map.put(10,"w_state");map.put(11,"w_zip");
			map.put(12,"w_country");map.put(13,"w_gm_offset");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load warehouse data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}

