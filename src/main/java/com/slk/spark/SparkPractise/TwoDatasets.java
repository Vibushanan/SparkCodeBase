package com.slk.spark.SparkPractise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TwoDatasets {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> File1 = sc.textFile("C:/Users/vibushanan.somasunda/Desktop/SampleIP.txt", 2);
		JavaRDD<String> File2 = sc.textFile("C:/Users/vibushanan.somasunda/Desktop/joined.txt", 2);
		
		JavaPairRDD<String, String> age1 = File1.mapToPair(new PairFunction<String,String,String>(){

			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] tokens = arg0.split(",");
				return new Tuple2(tokens[0],tokens[1]);
			}
			
		});
		
		
		JavaPairRDD<String, String> relations = File2.mapToPair(new PairFunction<String,String,String>(){

			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] tokens = arg0.split(",");
				return new Tuple2(tokens[0],tokens[1]);
			}
			
		});
		
		System.out.println(age1.collect());
		System.out.println(relations.collect());
		
		JavaPairRDD<String, Tuple2<String, String>> joined = relations.join(age1);
		
		JavaRDD<Tuple2<String, String>> joinedrel = 	joined.map(new Function<Tuple2<String,Tuple2<String,String>>,Tuple2<String,String>>(){

			public Tuple2<String, String> call(
					Tuple2<String, Tuple2<String, String>> arg0)
					throws Exception {
				
				Tuple2<String, String> tup = arg0._2();
				
				
				return new Tuple2(tup._1,arg0._1);
			}
			
		});
		
		System.out.println(joinedrel.collect());
		
		
		System.out.println("-----------Co  Group--------------");
		
		
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogrouped = age1.cogroup(relations);
		
		System.out.println(cogrouped.collect());
		
	}

}
