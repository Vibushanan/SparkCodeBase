package com.slk.spark.SparkPractise;

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

public class SampleOperations {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		System.out.println(sc);
		
		JavaRDD<String> distFile = sc.textFile("C:/Users/vibushanan.somasunda/Desktop/SampleIP.txt", 2);
		
		JavaPairRDD<String, Integer> place = distFile.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String arg0) throws Exception {
				
				String[] tokens = arg0.split(",");
				
				return new Tuple2(tokens[2], 1);
			}
			
		});
		
		
	
		
		
		JavaRDD<String> ageq = distFile.mapPartitions(new FlatMapFunction<Iterator<String>,String>(){

			public Iterator<String> call(Iterator<String> arg0)
					throws Exception {
				List<String> age = new ArrayList<String>();
				while(arg0.hasNext()){
					String[] tok = arg0.next().split(",");
					
					age.add(tok[1]);
				}
				
				// TODO Auto-generated method stub
				return age.iterator();
			}
			
			
		});
		
		System.out.println(ageq.getNumPartitions());

		
		System.out.println(ageq.collect());
		
		JavaPairRDD<String, Integer> placeAggregate = place.reduceByKey(new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		}, 4);
		
		
		
		place.aggregateByKey(0, new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		}, new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		});
		
		
		
System.out.println(placeAggregate.getNumPartitions());

		
		System.out.println(placeAggregate.collect());
	}

}
