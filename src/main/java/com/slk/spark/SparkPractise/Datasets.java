package com.slk.spark.SparkPractise;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Datasets {

	public static void main(String[] args) {

SparkSession spark = SparkSession
  .builder().master("local[*]").config("spark.sql.warehouse.dir", "file///D:/Warehouse")
  .appName("Java Spark SQL Example").getOrCreate();



Dataset<String> df = spark.read().textFile("D:/Warehouse/File/SampleIP.txt");


//DataFrame
Dataset<Row> df1 = spark.read().json("sss.json");

String[] cols = df1.columns();

df1.createOrReplaceTempView("family");
//System.out.println("SQL");
Dataset<Row> sqlDF  =spark.sql("Select * from family");
//sqlDF.show();
//df1.show();


System.out.println("-------------------RES-----------------------------------");
df1.select(df1.col("name"),df1.col("tags")).where(df1.col("name").notEqual("A green door")).show();
System.out.println("------------------------------------------------------");

df1.createOrReplaceTempView("sample");

Dataset<Row> count = spark.sql("Select count(*) from sample");

count.show();


//System.out.println(df.first()
		//);
	}

}
