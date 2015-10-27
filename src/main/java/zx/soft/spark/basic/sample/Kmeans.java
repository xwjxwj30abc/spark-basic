package zx.soft.spark.basic.sample;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class Kmeans {

	private static class ParsePoint implements Function<String, Vector> {

		private static final Pattern SPACE = Pattern.compile(",");

		@Override
		public Vector call(String v1) throws Exception {
			String[] tok = SPACE.split(v1);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}
			return Vectors.dense(point);
		}
	}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("mlib_KMeans");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("hdfs://bigdata4:8020/user/spark/KMeansData.txt");
		JavaRDD<Vector> points = lines.map(new ParsePoint());
		KMeansModel model = KMeans.train(points.rdd(), 3, 10, 1, KMeans.K_MEANS_PARALLEL());
		System.out.println("Center:");
		for (Vector center : model.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = model.computeCost(points.rdd());
		System.out.println("Cost: " + cost);
		sc.stop();
	}
}
