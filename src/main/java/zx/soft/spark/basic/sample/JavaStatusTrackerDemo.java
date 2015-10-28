package zx.soft.spark.basic.sample;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class JavaStatusTrackerDemo {

	public static final class IdentityWithDelay<T> implements Function<T, T> {

		@Override
		public T call(T v1) throws Exception {
			Thread.sleep(2 * 1000);
			return v1;
		}

	}

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaStatussTrackerDemo");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(new IdentityWithDelay<Integer>());
		JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
		while (!jobFuture.isDone()) {
			Thread.sleep(1000);
			List<Integer> jobIds = jobFuture.jobIds();
			if (jobIds.isEmpty()) {
				continue;
			}
			int currentJobId = jobIds.get(jobIds.size() - 1);
			SparkJobInfo jobInfo = sc.statusTracker().getJobInfo(currentJobId);
			SparkStageInfo stateInfo = sc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
			System.out.println(stateInfo.numActiveTasks() + " tasks total:" + stateInfo.numActiveTasks() + " active,"
					+ stateInfo.numCompletedTasks() + " complete");
		}
		System.out.println("Job results are:" + jobFuture.get());
		sc.stop();
	}
}
