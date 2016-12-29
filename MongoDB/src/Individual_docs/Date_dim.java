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


public class Date_dim {
	public Date_dim(DB db){
	
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/date_dim.dat");
			DBCollection collection = db.createCollection("date_dim", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"d_date_sk");map.put(1,"d_date_id");map.put(2,"d_date");
			map.put(3,"d_month_seq");map.put(4,"d_week_seq");map.put(5,"d_quarter_seq");
			map.put(6,"d_year");map.put(7,"d_dow");map.put(8,"d_moy");
			map.put(9,"d_dom");map.put(10,"d_qoy");map.put(11,"d_fy_year");
			map.put(12,"d_fy_quarter_seq");map.put(13,"d_fy_week_seq");map.put(14,"d_day_name");
			map.put(15,"d_quarter_name");map.put(16,"d_holiday");map.put(17,"d_weekend");
			map.put(18,"d_following_holiday");map.put(19,"d_first_dom");map.put(20,"d_last_dom");
			map.put(21,"d_same_day_ly");map.put(22,"d_same_day_lq");map.put(23,"d_current_day");
			map.put(24,"d_current_week");map.put(25,"d_current_month");
			map.put(26,"d_current_quarter");map.put(27,"d_current_year");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load date dimension data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
