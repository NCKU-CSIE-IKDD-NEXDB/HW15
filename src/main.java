import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;
import com.mongodb.Mongo;  
import com.mongodb.DB;  
import com.mongodb.DBCollection;  
import com.mongodb.BasicDBObject;  
import com.mongodb.DBObject;  
import com.mongodb.DBCursor;

public class main {
	@SuppressWarnings({ "unchecked" })
	public static ArrayList<String>[] user=new ArrayList[6];
	public static int folder_num;
	public static void listFilesForFolder(final File folder){
		//that make all file name to arraylist
		System.out.println(folder.getAbsolutePath());
		for(final File fileEntry : folder.listFiles()){
			if(fileEntry.isDirectory()){
				listFilesForFolder(fileEntry);
			}else{
				user[folder_num].add(fileEntry.getPath());
			}
		}
	}
	//save the entry by its data and its user
	public static void postgresql_insert() throws IOException, SQLException, ClassNotFoundException{
		Connection c = null;
		Statement stmt = null;
		Class.forName("org.postgresql.Driver");
		c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/IKDD_NEXDB",
				"hank", "hanklgs");
		System.out.println("open successly");
		String sql,line;
		for(int i=0;i<6;i++){	//for each user
			for(int y=0; y<user[i].size();y++){	//for each file in user folder
				BufferedReader reader = new BufferedReader(new FileReader(user[i].get(y)));
				String[] value;
				for(int x=0;x<6;x++)
					line=reader.readLine();
				while( (line =reader.readLine()) != null){	//read the file and insert the data
					value=line.split(",");
					sql="insert into user_record(name, posx, posy, data1, data2, data3, time) values('user_"
						+ i + "', '"+value[0]+"', '"+value[1]
						+ "', '"+value[2]+"', '"+value[3]+"', '"+value[4]
						+ "', '"+value[5]+" "+value[6]+"');";
					try {
						stmt = c.createStatement();
						stmt.executeUpdate(sql);
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}	//while
				reader.close();
			}
		}//for int i=0;i<6; i++
		c.close();
	}
	
	public static void mongodb_insert() throws IOException{
		Mongo m =new Mongo();
		DB db =m.getDB("mydb");
		DBCollection coll = db.getCollection("mydb");
		BasicDBObject doc = new BasicDBObject();
		String line;
		for(int i=0;i<6;i++){	//for each user
			for(int y=0; y<user[i].size();y++){	//for each file in user folder
				BufferedReader reader = new BufferedReader(new FileReader(user[i].get(y)));
				String[] value;
				for(int x=0;x<6;x++)
					line=reader.readLine();
				while( (line =reader.readLine()) != null){	//read the data and insert
					value=line.split(",");
					doc.clear();
					doc.put("name", "user_"+i);
					doc.put("posx", value[0]);
					doc.put("posy", value[1]);
					doc.put("date1",value[2]);
					doc.put("data2",value[3]);
					doc.put("data3",value[4]);
					doc.put("time", value[5]+" "+value[6]);
					coll.insert(doc);
				}	//while
				reader.close();
			}
		}
		
	}
	
	public static void postgre_query() throws ClassNotFoundException, SQLException{
		Connection c = null;
		Statement stmt = null;
		Class.forName("org.postgresql.Driver");
		c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/IKDD_NEXDB",
				"hank", "hanklgs");
		System.out.println("open successly");
		String sql="select * from user_record where name ='user_3' order by time;";
		stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		stmt.close();
	}
	
	public static void mongodb_query() throws UnknownHostException{
		Mongo m =new Mongo();
		DB db =m.getDB("mydb");
		DBObject doc = new BasicDBObject();
		DBObject sort = new BasicDBObject();
		DBCursor rs=db.getCollection("mydb").find((DBObject)doc.put("name", "user_3")).sort((DBObject)sort.put("time",-1));
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		double post_time, mongo_time;
		long startTime,endTime;
		for(int i=0;i<6;i++)
			user[i]=new ArrayList<String>();
		folder_num=0;
		File folder;
		for(folder_num=0; folder_num<6; folder_num++){	//open the file
			folder = new File("/home/hank/HW12/Geolife Trajectories 1.3/Data/00"+folder_num+"/Trajectory");
			listFilesForFolder(folder);
		}
		startTime =System.nanoTime();	//run insert postgresql table
		postgresql_insert();
		endTime = System.nanoTime();
		post_time=(endTime - startTime)/1e9;
		
		System.out.println("start mongodb");
		startTime =System.nanoTime();	//run insert mongodb collection
		mongodb_insert();
		endTime = System.nanoTime();
		mongo_time=(endTime - startTime)/1e9;
		System.out.println("postgresql insert time:"+post_time);
		System.out.println("mongodb insert time   :"+mongo_time);
		
		startTime =System.nanoTime();	//run query posgresql
		postgre_query();
		endTime = System.nanoTime();
		post_time=(endTime - startTime)/1e9;
		
		startTime =System.nanoTime();	//run query mongodb
		mongodb_query();
		endTime = System.nanoTime();
		mongo_time=(endTime - startTime)/1e9;
		System.out.println("postgresql query time:"+post_time);
		System.out.println("mongodb query time   :"+mongo_time);
	}

}
