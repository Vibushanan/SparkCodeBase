package rddOperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BasicOperations {

	public static void main(String args[]){
	// TODO Auto-generated method stub
	SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);

	
	JavaRDD<String> distFile = sc.textFile("C:/Users/vibushanan.somasunda/Desktop/SampleIP.txt", 2);
	
	JavaPairRDD<String, Integer> place = distFile.mapToPair(new PairFunction<String,String,Integer>(){

		public Tuple2<String, Integer> call(String arg0) throws Exception {
			
			String[] tokens = arg0.split(",");
			
			return new Tuple2(tokens[2], 1);
		}
		
	});
	
	place.collect();
	
	}
}
