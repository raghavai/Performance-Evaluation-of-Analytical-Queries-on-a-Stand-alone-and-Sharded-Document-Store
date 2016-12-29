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


public class Customer_demographics {
	public Customer_demographics(DB db){
		
		try{
		
			BufferedReader br = null;
	
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/customer_demographics.dat");
			DBCollection collection = db.createCollection("customer_demographics", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"cd_demo_sk");map.put(1,"cd_gender");map.put(2,"cd_marital_status");
			map.put(3,"cd_education_status");map.put(4,"cd_purchase_estimate");map.put(5,"cd_credit_rating");
			map.put(6,"cd_dep_count");map.put(7,"cd_dep_employed_count");map.put(8,"cd_dep_college_count");
			
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

