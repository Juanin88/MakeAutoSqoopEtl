
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class Maker {

	/*
	 * 
	 * Before Running this example we should start thrift server. To Start
	 * Thrift server we should run below command in terminal 
	 * hive --service hiveserver &
	 */
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws IOException, SQLException {
		
		int twitter_hashtags = 0;
		int twitter_tweets = 0;
		int twitter_tweets_filtro_palabra = 0;
		int twitter_user = 0;
		
		// TODO Auto-generated method stub
		Properties props = new Properties();
		FileInputStream fis = null;
		fis = new FileInputStream("db.properties");
		props.load(fis);

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection(
				props.getProperty("hiveUrl")+"/social_network");
		
		Statement stmt = con.createStatement();

		String sql;
		sql = "select 'twitter_tweets' as tabla, max(id) from twitter_tweets union"
				+ " select 'twitter_hashtags' as tabla, max(id) from twitter_hashtags union"
				+ " select 'twitter_tweets_filtro_palabra' as tabla, max(id) from twitter_tweets_filtro_palabra union"
				+ " select 'twitter_user' as tabla, max(id) from twitter_user";
		ResultSet res;
		res = stmt.executeQuery(sql);
		// show tables
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		
		while (res.next()) {
			
			if (res.getString(1).equals("twitter_hashtags")) {
				twitter_hashtags = Integer.parseInt( res.getString(2) );
			}
			if (res.getString(1).equals("twitter_tweets")) {
				twitter_tweets = Integer.parseInt( res.getString(2) );
			}
			if (res.getString(1).equals("twitter_tweets_filtro_palabra")) {
				twitter_tweets_filtro_palabra = Integer.parseInt( res.getString(2) );
			}
			if (res.getString(1).equals("twitter_user")) {
				twitter_user = Integer.parseInt( res.getString(2) );
			}
			
			System.out.println(res.getString(1)+" - "+res.getString(2));
			
		}

		res.close();
		stmt.close();
		con.close();
		
		File file = new File(props.getProperty("path"));

		file.delete();
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
			file.setExecutable(true);
			file.setWritable(true);
			file.setReadable(true);
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String bash = "";
		
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
				+ "--last-value "+twitter_tweets+" "
				+ "--driver com.mysql.jdbc.Driver";
		
		String bash_twitter_hashtags = "sqoop import "
				+ "--connect "+props.getProperty("url")+" "
				+ "--username "+props.getProperty("username")+" "
				+ "--password "+props.getProperty("password")+" "
				+ "--split-by id "
				+ "--columns id,id_tweet,id_user,hashtag "
				+ "--table twitter_hashtags "
				+ "--hive-import "
				+ "--hive-table social_network.twitter_hashtags "
				+ "--incremental append "
				+ "--check-column id "
				+ "--last-value "+twitter_hashtags+" "
				+ "--driver com.mysql.jdbc.Driver";
		
		String bash_twitter_tweets_filtro_palabra = "sqoop import "
				+ "--connect "+props.getProperty("url")+" "
				+ "--username "+props.getProperty("username")+" "
				+ "--password "+props.getProperty("password")+" "
				+ "--split-by id "
				+ "--columns id,id_filtro_palabra,id_tweet "
				+ "--table twitter_tweets_filtro_palabra "
				+ "--hive-import "
				+ "--hive-table social_network.twitter_tweets_filtro_palabra "
				+ "--incremental append "
				+ "--check-column id "
				+ "--last-value "+twitter_tweets_filtro_palabra+" "
				+ "--driver com.mysql.jdbc.Driver";
		
		String bash_twitter_user = "sqoop import "
				+ "--connect "+props.getProperty("url")+" "
				+ "--username "+props.getProperty("username")+" "
				+ "--password "+props.getProperty("password")+" "
				+ "--split-by id "
				+ "--columns id,id_user,screen_name,real_name,description,friends_count,followers_count,location "
				+ "--table twitter_user "
				+ "--hive-import "
				+ "--hive-table social_network.twitter_user "
				+ "--incremental append "
				+ "--check-column id "
				+ "--last-value "+twitter_user+" "
				+ "--driver com.mysql.jdbc.Driver";


		bash = bash_twitter_hashtags + " && "
				+ bash_twitter_tweets + " && "
				+ bash_twitter_tweets_filtro_palabra + " && "
				+ bash_twitter_user;
		
		bw.write(bash);
		bw.write(System.getProperty("line.separator"));
		bw.close();
		System.out.println("Archivo .sh generado.");
		


		ProcessBuilder pb = new ProcessBuilder(props.getProperty("pathBash"));
		Process p = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
		    System.out.println(line);
		}


		
		
	}

	
}
