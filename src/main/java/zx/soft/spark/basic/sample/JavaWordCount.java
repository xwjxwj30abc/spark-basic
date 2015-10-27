package zx.soft.spark.basic.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class JavaWordCount {

	public static Logger logger = LoggerFactory.getLogger(JavaWordCount.class);

	public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark　Word Count"));
		final int threshold = 2;

		// split each document into words
		JavaRDD<String> tokenized = sc.textFile("hdfs://bigdata4:8020/user/spark/data.txt").flatMap(
				new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(s.split(" "));
					}
				});

		//count the occurrence of each word

		JavaPairRDD<String, Integer> pairs = tokenized.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		// filter out words with less than threshold occurrences
		JavaPairRDD<String, Integer> filtered = counts.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Integer> tup) {
				return tup._2() >= threshold;
			}
		});

		// count characters
		JavaPairRDD<Character, Integer> charCounts = filtered
				.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Character>() {
					@Override
					public Iterable<Character> call(Tuple2<String, Integer> s) {
						Collection<Character> chars = new ArrayList<Character>(s._1().length());
						for (char c : s._1().toCharArray()) {
							chars.add(c);
						}
						return chars;
					}
				}).mapToPair(new PairFunction<Character, Character, Integer>() {
					@Override
					public Tuple2<Character, Integer> call(Character c) {
						return new Tuple2<Character, Integer>(c, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		System.out.println(charCounts.collect());

	}
}
