package com.slk.spark.SparkPractise;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Datasets {

	public static void main(String[] args) {

SparkSession spark = SparkSession
  .builder().master("local[*]").config("spark.sql.warehouse.dir", "file///C:/Users/vibushanan.somasunda/Desktop")
  .appName("Java Spark SQL Example").getOrCreate();


Dataset<String> df = spark.read().textFile("SampleIP.txt");

Dataset<Row> df1 = spark.read().json("sss.json");
String[] cols = df1.columns();

df1.createOrReplaceTempView("family");
System.out.println("SQL");
Dataset<Row> sqlDF  =spark.sql("Select * from family");
sqlDF.show();
df1.show();

df1.select(df1.col("name"),df1.col("tags")).show();

System.out.println(df.first()
		);
	}

}
