package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;


public class Store_returns {
	public Store_returns(DB db){
		
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/store_returns.dat");
			DBCollection collection = db.createCollection("store_returns", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"sr_returned_date_sk");map.put(1,"sr_return_time_sk");map.put(2,"sr_item_sk");
			map.put(3,"sr_customer_sk");map.put(4,"sr_cdemo_sk");map.put(5,"sr_hdemo_sk");
			map.put(6,"sr_addr_sk");map.put(7,"sr_store_sk");map.put(8,"sr_reason_sk");
			map.put(9,"sr_ticket_number");map.put(10,"sr_return_quantity");map.put(11,"sr_return_amt");
			map.put(12,"sr_return_tax");map.put(13,"sr_return_amt_inc_tax");map.put(14,"sr_fee");
			map.put(15,"sr_return_ship_cost");map.put(16,"sr_refunded_cash");map.put(17,"sr_reversed_charge");
			map.put(18,"sr_store_credit");map.put(19,"sr_net_loss");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load store returns data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}

