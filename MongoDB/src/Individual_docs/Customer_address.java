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


public class Customer_address {
	public Customer_address(DB db){
		
	try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/customer_address.dat");
			DBCollection collection = db.createCollection("customer_address", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"ca_address_sk");map.put(1,"ca_address_id");map.put(2,"ca_street_number");
			map.put(3,"ca_street_name");map.put(4,"ca_street_type");map.put(5,"ca_suite_number");
			map.put(6,"ca_city");map.put(7,"ca_county");map.put(8,"ca_state");
			map.put(9,"ca_zip");map.put(10,"ca_country");map.put(11,"ca_gm_offset");
			map.put(12,"ca_location_type");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load customer address data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
