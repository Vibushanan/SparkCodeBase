package sqlSpark;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.DataFrameStatFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function1;
import scala.Serializable;

public class DataFrameBasics {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
	             .master("local")
	             .config("spark.sql.warehouse.dir", "D:/New folder (3)/SparkPractise/")		
	             .appName("Streaming")
	             .getOrCreate();

		
Dataset<Row> matchRecds = spark.read().csv("D:/Downloads/t20_csv/t20_csv/211028.csv").filter(col("_c0").$eq$eq$eq("ball"));

Dataset<String> matchRecds1  = spark.read().textFile("D:/Downloads/t20_csv/t20_csv/211028.csv");

JavaRDD<String> matchRecdsRDD = matchRecds1.toJavaRDD().filter(new Function<String,Boolean>(){

	public Boolean call(String arg0) throws Exception {
		
		String[] tokens = arg0.split(",");
		
		if(tokens[0].equals("ball")||tokens[0] == "ball"){
			return true;
		}
		return false;
	}
	
});


JavaRDD<Scoring> matchRecdsRDD1  = matchRecdsRDD.map(new Function<String,Scoring>(){

	public Scoring call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		
		String[] tokens = arg0.split(",");
		Scoring sc = new Scoring();
		
		sc.setBallno(Float.parseFloat(tokens[2]));
		sc.setBatCountry(tokens[3]);
		sc.setBatsMan1(tokens[4]);
		sc.setBatsMan2(tokens[5]);
		sc.setBowler(tokens[6]);
		sc.setBatsMan1Run(Integer.parseInt(tokens[7]));
		sc.setBatsMan1Run(Integer.parseInt(tokens[8]));
		return sc;
	}
	
});



Encoder<Scoring> personEncoder = Encoders.bean(Scoring.class);



Dataset<Row> onr = spark.createDataFrame(matchRecdsRDD1, Scoring.class);

onr.show();


/*onr.createOrReplaceTempView("Match1");

onr.cube(col("batCountry"),col("batsMan1Run")).count().show();*/

DataFrameStatFunctions stats = onr.where(col("BatCountry").$eq$eq$eq("England")).groupBy("Bowler").count().stat();





//spark.sql("Select  Bowler,Count(Bowler)/6 as Overs from Match1 where BatCountry = \"England\" group by Bowler").show();


	}
	public static class Scoring implements Serializable {
	
		private float Ballno;
		private String BatCountry;
		private String BatsMan1;
		private String BatsMan2;
		private String Bowler;
		private int BatsMan1Run;
		private int BatsMan2Run;
		
		public float getBallno() {
			return Ballno;
		}


		public void setBallno(float ballno) {
			Ballno = ballno;
		}


		public String getBatCountry() {
			return BatCountry;
		}


		public void setBatCountry(String batCountry) {
			BatCountry = batCountry;
		}


		public String getBatsMan1() {
			return BatsMan1;
		}


		public void setBatsMan1(String batsMan1) {
			BatsMan1 = batsMan1;
		}


		public String getBatsMan2() {
			return BatsMan2;
		}


		public void setBatsMan2(String batsMan2) {
			BatsMan2 = batsMan2;
		}


		public String getBowler() {
			return Bowler;
		}


		public void setBowler(String bowler) {
			Bowler = bowler;
		}


		public int getBatsMan1Run() {
			return BatsMan1Run;
		}


		public void setBatsMan1Run(int batsMan1Run) {
			BatsMan1Run = batsMan1Run;
		}


		public int getBatsMan2Run() {
			return BatsMan2Run;
		}


		public void setBatsMan2Run(int batsMan2Run) {
			BatsMan2Run = batsMan2Run;
		}


		
		
	
		
		}	

}
