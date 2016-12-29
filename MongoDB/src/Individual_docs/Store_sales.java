package Individual_docs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;


public class Store_sales {
	public Store_sales(DB db){
		
		try{
		
			BufferedReader br = null;
			
			//Opening a file and reading contents
			File newFile = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/store_sales.dat");
			DBCollection collection = db.createCollection("store_sales", null);
			br = new BufferedReader(new FileReader(newFile));
			String line = null;
			
			HashMap<Integer,String> map = new HashMap<Integer,String>();
			map.put(0,"ss_sold_date_sk");map.put(1,"ss_sold_time_sk");map.put(2,"ss_item_sk");
			map.put(3,"ss_customer_sk");map.put(4,"ss_cdemo_sk");map.put(5,"ss_hdemo_sk");
			map.put(6,"ss_addr_sk");map.put(7,"ss_store_sk");map.put(8,"ss_promo_sk");
			map.put(9,"ss_ticket_number");map.put(10,"ss_quantity");map.put(11,"ss_wholesale_cost");
			map.put(12,"ss_list_price");map.put(13,"ss_sales_price");map.put(14,"ss_ext_discount_amt");
			map.put(15,"ss_ext_sales_price");map.put(16,"ss_ext_wholesale_cost");map.put(17,"ss_ext_list_price");
			map.put(18,"ss_ext_tax");map.put(19,"ss_coupon_amt");map.put(20,"ss_net_paid");
			map.put(21,"ss_net_paid_inc_tax");map.put(22,"ss_net_profit");
			
			for(Integer key:map.keySet())
				System.out.println(key+" "+map.get(key));
			
			long startTime = System.currentTimeMillis();
			new Load(br,collection,map);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time to load store sales data: "+(endTime-startTime)+" milli seconds");

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}

