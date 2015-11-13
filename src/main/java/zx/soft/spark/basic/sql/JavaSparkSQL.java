package zx.soft.spark.basic.sql;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JavaSparkSQL {

	public static class Person implements Serializable {

		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public int getAge() {
			return age;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setAge(int age) {
			this.age = age;
		}

	}

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("javaSparkSQL");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		//版本不兼容导致SQLContext初始化在运行时出问题
		SQLContext sqlContext = new SQLContext(ctx);
		JavaRDD<Person> people = ctx.textFile("/user/hdfs/people.txt").map(new Function<String, Person>() {

			@Override
			public Person call(String v1) throws Exception {
				String[] parts = v1.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}
		});

		DataFrame schemaPerson = sqlContext.createDataFrame(people, Person.class);
		schemaPerson.registerTempTable("people");
		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >=10");

		List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row v1) throws Exception {

				return "Name:" + v1.getString(0);
			}

		}).collect();

		for (String name : teenagerNames) {
			System.out.println(name);
		}

		schemaPerson.write().parquet("/user/hdfs/people.parquet");
		DataFrame parquetFile = sqlContext.read().parquet("/user/hdfs/people.parquet");

		parquetFile.registerTempTable("parquetFile");
		DataFrame teenagers2 = sqlContext.sql("SELECT name FROM people WHERE age >=10");
		teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Name:" + row.getString(0);
			}
		}).collect();

		for (String name : teenagerNames) {
			System.out.println(name);
		}

	}
}
