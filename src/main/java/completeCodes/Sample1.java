package completeCodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import com.oracle.jrockit.jfr.DataType;
import com.slk.spark.SparkPractise.JavaEncoders.Person;

public class Sample1 {

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
		 .builder().master("local[*]")
		.config("spark.sql.warehouse.dir", "file///C:/Users/vibushanan.somasunda/Desktop")			  
		 .appName("Java Spark SQL Example").getOrCreate();
		 
		/*
		SparkConf conf = new SparkConf().setAppName("Sample").set("spark.driver.allowMultipleContexts","true").setMaster(
				"local[*]");*/
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		
		// Reading as RDD

		Encoder<Person> personEndoder = Encoders.bean(Person.class);

		JavaRDD<Person> personRDD = sc.textFile(
				"D:/New folder (3)/SparkPractise/person.json")

		.map(new Function<String, Person>() {

			public Person call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObj = new JSONObject(arg0);
				Person person = new Person();
				person.setAge(jsonObj.getInt("age"));
				person.setName(jsonObj.getString("name"));
				return person;
			}

		});
		
		
		
		JavaRDD<Row> personROW = personRDD.map(new Function<Person,Row>(){

			public Row call(Person arg0) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(arg0.getName(),arg0.getAge());
			}
			
		});	
		//RDD to DataFrames
		
	
	Dataset<Row> personRow =  spark.createDataFrame(personRDD, Person.class);

	personRow.show();
	
	
	personRow.filter(personRow.col("age").$greater(20)).show();
	
	//DataFrame to Datset
	Dataset<Person> personDS = personRow.filter(personRow.col("age").$greater(20)).as(personEndoder);
	
	
	personDS.select(personDS.col("age")).show();
	
	
	//Create DataFrame With out bean obj
	
	
	String fields = "name,age";
	
	
	List<StructField> Structfields = new ArrayList<StructField>();
	
	
	
		
		StructField fld = DataTypes.createStructField("name",DataTypes.StringType, false);
		StructField fld2 = DataTypes.createStructField("age",DataTypes.IntegerType, false);
		Structfields.add(fld);
		Structfields.add(fld2);
	
	StructType schema = DataTypes.createStructType(Structfields);
	
	spark.createDataFrame(personROW, schema).show();
	
	}

}
