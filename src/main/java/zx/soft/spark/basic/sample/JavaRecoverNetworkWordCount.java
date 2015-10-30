package zx.soft.spark.basic.sample;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class JavaRecoverNetworkWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	private static JavaStreamingContext createContext(String ip, int port, String checkpointDirectory, String outputPath) {
		System.out.println("Creating new context");
		//		final File outputFile = new File(outputPath);
		//		if (outputFile.exists()) {
		//			outputFile.delete();
		//		}
		SparkConf sparkConf = new SparkConf().setAppName("JavaRecoverNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
		ssc.checkpoint(checkpointDirectory);
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {

				return Lists.newArrayList(SPACE.split(t));
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
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
		wordCounts.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

			@Override
			public Void call(JavaPairRDD<String, Integer> rdd, Time time) throws Exception {
				String counts = "Counts at time " + time + " " + rdd.collect();
				System.out.println(counts);
				//System.out.println("Appending to " + outputFile.getAbsolutePath());
				rdd.saveAsTextFile("/user/hdfs/output/" + System.currentTimeMillis());
				//Files.append(counts + "\n", outputFile, Charset.defaultCharset());
				return null;
			}

		});
		return ssc;
	}

	public static void main(String[] args) {
		final String ip = args[0];
		final int port = Integer.parseInt(args[1]);
		final String checkpointDirectory = args[2];
		final String outputPath = args[3];
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(ip, port, checkpointDirectory, outputPath);
			}
		};
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
		ssc.start();
		ssc.awaitTermination();
		ssc.stop();
	}
}
