package zx.soft.spark.basic.sample;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class JavaNetworkWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], 9999, StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Lists.newArrayList(SPACE.split(t));
			}

		});

		JavaPairDStream<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}

		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}

		});
		wordCount.print();
		ssc.start();
		ssc.awaitTermination();

	}
}
