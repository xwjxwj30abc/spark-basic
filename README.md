###运行方法

1. 上传输入文本到hdfs(namenode节点上操作)
   sudo -u hdfs hadoop  fs -put input.txt sina/input

2. 打包mvn package 并上传到namenode节点主机

3. spark-submit --class zx.soft.spark.demo.JavaWordCount  spark-demo-0.0.1-SNAPSHOT.jar /user/hdfs/sina/input(输入待word count的文本路径) 2(阈值过滤参数)
 
