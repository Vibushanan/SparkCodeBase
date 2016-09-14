package com.slk.spark.SparkPractise;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class Stateless {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = 
				new SparkConf().setMaster("local[2]").setAppName("WC")
				.set("spark.akka.heartbeat.interval", "400").set("spark.driver.allowMultipleContexts", "true").set("spark.cassandra.connection.host","10.172.20.29");
		
		
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(4));
		
	
		
		
		ClassTag<LongWritable> k = scala.reflect.ClassTag$.MODULE$.apply(LongWritable.class);
		ClassTag<Text> v = scala.reflect.ClassTag$.MODULE$.apply(Text.class);
		ClassTag<InputFormat<LongWritable, Text>> f = scala.reflect.ClassTag$.MODULE$.apply(TextInputFormat.class);

		
		JavaDStream<String> lines =jssc.textFileStream("file:///D:/Streaming/Data/Input/");
		
		
		
		lines.dstream().print();

		
		
		JavaPairDStream<String, Integer> movieCount = lines.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				String[] tokens = t.split(",");
				return new Tuple2(tokens[0],Integer.parseInt(tokens[1]));
				
			}
			
		});
		
		
		JavaPairDStream<Tuple2<String, Iterable<Integer>>, Long> movieCount3 =  movieCount.groupByKey().countByValue();
		movieCount3.print();
		
		jssc.start();              // Start the computation
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

