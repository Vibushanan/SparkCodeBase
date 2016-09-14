package streamingApplications;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class T20Analysis {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession spark = SparkSession.builder()
				             .master("local")
				             .config("spark.sql.warehouse.dir", "D:/New folder (3)/SparkPractise/")		
				             .appName("Streaming")
				             .getOrCreate();
		
		
 Dataset<Row> matchRecds = spark.read().csv("D:/Downloads/t20_csv/t20_csv/211028.csv").filter(col("_c0").$eq$eq$eq("ball"));
 
 Dataset<Row> matchRecds1 = spark.read().text("D:/Downloads/t20_csv/t20_csv/211028.csv");
 
 
 matchRecds1.printSchema();
 
 matchRecds1.show();
 
 
 
		
	}

}
