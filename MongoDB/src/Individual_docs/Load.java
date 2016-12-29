package Individual_docs;
import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;


public class Load {
	public Load(BufferedReader br,DBCollection collection, HashMap<Integer,String> map){
		
		try{
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
	
			String line=null;

			while((line = br.readLine())!=null){
				String arr[] = line.split("\\|",-1);
				int i;
				String regex_int = "^-*[0-9]+";
				String regex_decimal = "^-*[0-9]+\\.[0-9]+";
				String regex_date = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
				BasicDBObject doc = new BasicDBObject();
				
				for(i=0; i<arr.length; i++){
					if(arr[i].equals(""))
						continue;
					else if(arr[i].matches(regex_int))
						doc.append(map.get(i),Integer.parseInt(arr[i]));
					else if(arr[i].matches(regex_decimal))
						doc.append(map.get(i),Double.parseDouble(arr[i]));
					else if(arr[i].matches(regex_date))
						doc.append(map.get(i),formatter.format(formatter.parse(arr[i])));
					else 
						doc.append(map.get(i), arr[i]);
				}
				collection.insert(doc);
			}
	
		}
		catch(IOException e){
			e.printStackTrace();
		}
		catch(ParseException e){
			e.printStackTrace();
		}
	}
}
