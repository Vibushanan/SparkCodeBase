package completeCodes;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreaming {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		SparkSession spark = SparkSession
				 .builder().master("local[*]")
				.config("spark.sql.warehouse.dir", "D:/New folder (3)/SparkPractise/")			  
				 .appName("Java Spark SQL Example").getOrCreate();
		
		
		Dataset<Row> lines  = spark.readStream().text("file:///D:/Streaming/Data/sss.txt");
		
		Dataset<String> words = lines
			    .as(Encoders.STRING())
			    .flatMap(
			        new FlatMapFunction<String, String>() {
			          public Iterator<String> call(String x) {
			            return Arrays.asList(x.split(",")).iterator();
			          }
			        }, Encoders.STRING());
		
		
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
	/*SparkConf conf = new SparkConf().setAppName("Streaming App").setMaster("local[*]");
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaDStream<Tuple2> sata =  ssc.textFileStream("D:/Git/Stream/")	 
		
		 .map(new Function<String,Tuple2>(){

			public Tuple2 call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				String[] tokens= arg0.split(",");
				return new Tuple2(tokens[1],1);
			}
			 
		 });
		 
		 sata.dstream().print();
		
		sata.print();*/
		StreamingQuery query = wordCounts.writeStream().outputMode("complete")
				  .format("console")
				  .start();

				query.awaitTermination();
		
		 
		/*spark.start();
		 try {
			 spark.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

}
