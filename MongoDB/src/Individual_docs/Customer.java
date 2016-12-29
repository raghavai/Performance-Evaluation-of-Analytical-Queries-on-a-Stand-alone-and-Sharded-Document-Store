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


public class Customer {
	public Customer(DB db){
		
		try{
			
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/customer.dat");
			DBCollection collection = db.createCollection("customer", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"c_customer_sk");map.put(1,"c_customer_id");map.put(2,"c_current_cdemo_sk");
			map.put(3,"c_current_hdemo_sk");map.put(4,"c_current_addr_sk");map.put(5,"c_first_shipto_date_sk");
			map.put(6,"c_first_sales_date_sk");map.put(7,"c_salutation");map.put(8,"c_first_name");
			map.put(9,"c_last_name");map.put(10,"c_preferred_cust_flag");map.put(11,"c_bith_day");
			map.put(12,"c_birth_month");map.put(13,"c_birth_year");map.put(14,"c_birth_country");
			map.put(15,"c_login");map.put(16,"c_email_address");map.put(17,"c_last_review_date_sk");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load customer data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
