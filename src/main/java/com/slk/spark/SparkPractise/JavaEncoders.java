package com.slk.spark.SparkPractise;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class JavaEncoders {

	public static class Person implements Serializable {
		  private String name;
		  private int age;

		  public String getName() {
		    return name;
		  }

		  public void setName(String name) {
		    this.name = name;
		  }

		  public int getAge() {
		    return age;
		  }

		  public void setAge(int age) {
		    this.age = age;
		  }
		}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				  .builder().master("local[*]").config("spark.sql.warehouse.dir", "file///C:/Users/vibushanan.somasunda/Desktop")
				  .appName("Java Spark SQL Example").getOrCreate();
	

		
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		
		
		
		JavaRDD<String>  rdd = sc.textFile("person.json");
		
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		
		Dataset<Person> df1 = spark.read().json("person.json").as(personEncoder);
		
		
		Dataset<Row> personrdd = spark.read().json("person.json");
	
		
 df1.map(new MapFunction<Person,String>(){

	public String call(Person value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	 
 }, Encoders.STRING());
		
		
		
		/*Dataset<Person> df2 = df1.alias("new Person");
		
		
		df2.show();
		
		Column i =df2.apply("age");
		
		df2.show();
		
		System.out.println(i.toString());*/
		
	}

}
