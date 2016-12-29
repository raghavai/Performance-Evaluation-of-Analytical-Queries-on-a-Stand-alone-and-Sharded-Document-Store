package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;


public class Time_dim {
	public Time_dim(DB db){
		
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/time_dim.dat");
			DBCollection collection = db.createCollection("time_dim", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"t_time_sk");map.put(1,"t_time_id");map.put(2,"t_time");
			map.put(3,"t_hour");map.put(4,"t_minute");map.put(5,"t_second");
			map.put(6,"t_am_pm");map.put(7,"t_shift");map.put(8,"t_sub_shift");
			map.put(9,"t_meal_time");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load time dimension data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
