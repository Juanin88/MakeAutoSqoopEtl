
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Maker {

	/*
	 * 
	 * Before Running this example we should start thrift server. To Start
	 * Thrift server we should run below command in terminal 
	 * hive --service hiveserver &
	 */
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws IOException, SQLException {
		
	
	        try {
	            Class.forName(driverName);
	          } catch (ClassNotFoundException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	            System.exit(1);
	          }
	          //replace "hive" here with the name of the user the queries should run as
	          Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
	          Statement stmt = con.createStatement();
	          String tableName = "testHiveDriverTable";
	          stmt.execute("drop table if exists " + tableName);
	          stmt.execute("create table " + tableName + " (key int, value string)");
	          // show tables
	          String sql = "show tables '" + tableName + "'";
	          System.out.println("Running: " + sql);
	          ResultSet res = stmt.executeQuery(sql);
	          if (res.next()) {
	            System.out.println(res.getString(1));
	          }
	             // describe table
	          sql = "describe " + tableName;
	          System.out.println("Running: " + sql);
	          res = stmt.executeQuery(sql);
	          while (res.next()) {
	            System.out.println(res.getString(1) + "\t" + res.getString(2));
	          }
		
		
		
		
		
		
		
		
		
//		// TODO Auto-generated method stub
//		Properties props = new Properties();
//		FileInputStream fis = null;
//		fis = new FileInputStream("db.properties");
//		props.load(fis);
//
//		try {
//			Class.forName(driverName);
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//			System.exit(1);
//		}
//
//		Connection con = DriverManager.getConnection(
//				props.getProperty("hiveUrl")+"/social_network", "hiveuser", "");
//		
//		Statement stmt = con.createStatement();
//
//		String tableName = "twitter_tweets";
//		String sql;
//		sql = "select max(id) as max_id from "+tableName;
//		ResultSet res;
//		res = stmt.executeQuery(sql);
//		// show tables
//		System.out.println("Running: " + sql);
//		res = stmt.executeQuery(sql);
//		while (res.next()) {
//			System.out.println(res.getString(1));
//		}
//		res.close();
//		stmt.close();
//		con.close();
		
/*		File file = new File(props.getProperty("path"));

		file.delete();
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}
		
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String bash_twitter_tweets = "sqoop import "
				+ "--connect "+props.getProperty("url")+" "
				+ "--username "+props.getProperty("username")+" "
				+ "--password "+props.getProperty("password")+" "
				+ "--split-by id "
				+ "--columns id,id_tweet,tweet,id_user,created,ciudad,sentimiento,id_twitter_filtro,filtro_valido "
				+ "--table twitter_tweets "
				+ "--hive-import "
				+ "--hive-table social_network.twitter_tweets "
				+ "--incremental append "
				+ "--check-column id "
				+ "--last-value 1810880 "
				+ "--driver com.mysql.jdbc.Driver";
		
		String bash_ciudades = "sqoop import "
				+ "--connect "+props.getProperty("url")+" "
				+ "--username "+props.getProperty("username")+" "
				+ "--password "+props.getProperty("password")+" "
				+ "--split-by id "
				+ "--columns id,ciudad,estado,latitud,longitud,censo_2010,censo_estimado_2015,radio "
				+ "--table ciudades "
				+ "--hive-import "
				+ "--hive-table social_network.ciudades "
				+ "--incremental append "
				+ "--check-column id "
				+ "--last-value 305 "
				+ "--driver com.mysql.jdbc.Driver";

		
		
		System.out.println("Test!!");
		bw.write(bash_ciudades);
		bw.write(System.getProperty("line.separator"));
		bw.close();
		*/
	}

}
