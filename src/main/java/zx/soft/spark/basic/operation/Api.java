package zx.soft.spark.basic.operation;


public class Api {

	public static void main(String[] args) {

		//		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("api test"));
		//		List<Integer> data = Arrays.asList(1, 2, 3, 4);
		//		JavaRDD<Integer> dataRDD = sc.parallelize(data);
		//		JavaRDD<Integer> dataRDD2 = sc.parallelize(data, 2);

		//Printing elements of an RDD
		//		dataRDD.collect().forEach(null);
		//		dataRDD.take(3).forEach(null);
		//
		//		JavaRDD<String> files = sc.textFile("/user/hdfs/sina/input");
		//		JavaRDD<String> files2 = sc.textFile("/user/hdfs/sina/*.txt");
		//		JavaRDD<String> files3 = sc.textFile("/user/hdfs/sina/*.gz");

		//way1:Passing Functions to Spark
		//JavaRDD<Integer> length = files.map(s -> s.length());
		//.persist(StorageLevel.MEMORY_ONLY());
		//int sumLength = length.reduce((a, b) -> a + b);
		//System.out.println(sumLength);

		//way2:Passing Functions to Spark
		//		JavaRDD<String> lines = sc.textFile("data.txt");
		//		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		//			@Override
		//			public Integer call(String s) {
		//				return s.length();
		//			}
		//		});
		//		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
		//			@Override
		//			public Integer call(Integer a, Integer b) {
		//				return a + b;
		//			}
		//		});

		//wordcount mapreduce1
		//JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
		//JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		//wordcount mapreduce2
		//		JavaPairRDD<String, Integer> counts2 = lines.mapToPair(new PairFunction<String, String, Integer>() {
		//			@Override
		//			public Tuple2<String, Integer> call(String s) {
		//				return new Tuple2<String, Integer>(s, 1);
		//			}
		//		}).reduceByKey((i1, i2) -> i1 + i2);

		//		sc.wholeTextFiles("/user/hdfs/sina/");
		//
		//		sc.sequenceFile("/user/hdfs/sina/", Text.class, Text.class);

	}
}
