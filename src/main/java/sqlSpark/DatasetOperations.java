package sqlSpark;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
public class DatasetOperations {

	public static void main(String[] args) {
		
		
			SparkSession spark = SparkSession.builder()
		             .master("local")
		             .config("spark.sql.warehouse.dir", "D:/New folder (3)/SparkPractise/")		
		             .appName("Streaming")
		             .getOrCreate();
			
			
			
			Dataset<Row> salDataset = spark.read().json("D:/dataforhadoop/Sample.txt");
			Dataset<Row> compDataset = spark.read().json("D:/dataforhadoop/Company.txt");
			
	
			
			//Aggregate function
			
			//salDataset.groupBy(col("age")).agg(org.apache.spark.sql.functions.sum(col("salary"))).show();

			//Cube - returns all possible combination in cube col and does aggregates
			
			salDataset.cube(col("age")).sum("salary").show();
			
			/*+----+-----------+
			| age|sum(salary)|
			+----+-----------+
			|null|     170000|
			|  29|      90000|
			|  30|      80000|
			+----+-----------+*/

			
			//salDataset.join(compDataset, salDataset.col("name").$eq$eq$eq(compDataset.col("name"))).select(salDataset.col("name"),compDataset.col("company")).dropDuplicates().orderBy(salDataset.col("name")).show();
	
		//	salDataset.rollup(col("age")).sum("salary").show();
			
		/*	+----+-----------+
			| age|sum(salary)|
			+----+-----------+
			|null|     170000|
			|  29|      90000|
			|  30|      80000|
			+----+-----------+*/
			
	
	}

}
