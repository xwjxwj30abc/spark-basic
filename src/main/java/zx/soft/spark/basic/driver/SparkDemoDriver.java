package zx.soft.spark.basic.driver;

import org.apache.hadoop.util.ProgramDriver;

public class SparkDemoDriver {

	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver driver = new ProgramDriver();
		try {
			driver.addClass("word_count", zx.soft.spark.basic.sample.JavaWordCount.class, "单词计数");
			driver.driver(args);
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		System.exit(exitCode);
	}
}
