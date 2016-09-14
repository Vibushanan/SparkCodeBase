package com.slk.spark.SparkPractise;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
	
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		/*Encoder<Integer> integerEncoder= Encoders.INT();
		
		Dataset<Integer> DSInt = spark.createDataset(Arrays.asList(1,2,3), integerEncoder);
	*/	
	
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		
		Dataset<Person> df1 = spark.read().json("person.json").as(personEncoder);
		
		
		Dataset<Row> personrdd = spark.read().json("person.json");
		
		df1.groupBy("age").min("age").show();
		df1.show();
		Dataset<Person> df2 = df1.alias("new Person");
		df2.show();
		
		Column i =df2.apply("a");
		
		System.out.println(i.toString());
		
	}

}
