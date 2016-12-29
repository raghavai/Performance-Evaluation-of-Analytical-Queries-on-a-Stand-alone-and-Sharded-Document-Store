package Individual_docs;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class InsertDocuments {

	public static void main(String[] args) {
		
		try{
			
			MongoClient mongoClient = new MongoClient("192.168.2.159",27017);
			DB db = mongoClient.getDB("Dataset_5GB");
			int i = 0;
			
			//Listing the files in a folder
			File folder = new File("/media/Mongo/TPCDSVersion1.3.1/tools/data_5GB/");
			File[] listofFiles = folder.listFiles();
			//Arrays.sort(listofFiles);
			
			long startTime = System.currentTimeMillis();
			
			for(File file : listofFiles){
				if(file.isFile()){
					
					System.out.println(file.getName());
					
					/*if(file.getName().equals("customer.dat"))
						new Customer(db);
					else if(file.getName().equals("customer_address.dat"))
						new Customer_address(db);
					else if(file.getName().equals("customer_demographics.dat"))
						new Customer_demographics(db);
					else if(file.getName().equals("date_dim.dat"))
						new Date_dim(db);
					else if(file.getName().equals("household_demographics.dat"))
						new Household_demographics(db);
					else if(file.getName().equals("income_band.dat"))
						new Income_band(db);*/
					if(file.getName().equals("inventory.dat"))
						new Inventory(db);
					/*else if(file.getName().equals("item.dat"))
						new Item(db);
					else if(file.getName().equals("promotion.dat"))
						new Promotion(db);
					else if(file.getName().equals("reason.dat"))
						new Reason(db);
					else if(file.getName().equals("store.dat"))
						new Store(db);
					else if(file.getName().equals("store_returns.dat"))
						new Store_returns(db);
					else if(file.getName().equals("store_sales.dat"))
						new Store_sales(db);
					else if(file.getName().equals("time_dim.dat"))
						new Time_dim(db);
					else if(file.getName().equals("warehouse.dat"))
						new Warehouse(db);*/
				}		
			}
			
			long endTime = System.currentTimeMillis();
			 
			long totalTime = endTime - startTime;
			System.out.println("Time to load 5 GB of data: "+totalTime+" milli seconds");
		}
			
		catch(IOException e){
			e.printStackTrace();
		}
		
	}

}
