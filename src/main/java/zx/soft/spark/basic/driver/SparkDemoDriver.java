package zx.soft.spark.basic.driver;

import zx.soft.spark.basic.sample.JavaWordCount;
import zx.soft.utils.driver.ProgramDriver;

public class SparkDemoDriver {

	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver driver = new ProgramDriver();
		try {
			driver.addClass("word_count", JavaWordCount.class, "单词计数");
			driver.driver(args);
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		System.exit(exitCode);
	}
}
