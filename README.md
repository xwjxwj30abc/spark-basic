##运行方法

###基本运行步骤
1. 上传输入文本到hdfs(namenode节点上操作)
   sudo -u hdfs hadoop  fs -put input.txt sina/input

2. 打包mvn package 并上传到namenode节点主机

3. spark-submit --class zx.soft.spark.demo.JavaWordCount  spark-demo-0.0.1-SNAPSHOT.jar /user/hdfs/sina/input(输入待word count的文本路径) 2(阈值过滤参数)
 
###运行JavaNetworkWordCount条件

1. 在装有netcat的机器运行　nc -lk 9999(启动9999端口测试)
2. 启动代码时传入该机器的ip：
sudo -u spark spark-submit --class zx.soft.spark.basic.sample.JavaNetworkWordCount --master yarn  spark-demo-0.0.1-SNAPSHOT.jar 192.168.6.126
3. 在nc端输入hello world 等，结果显示如下(代码中配置时间分段为10s)
-------------------------------------------
Time: 1357008430000 ms
-------------------------------------------
(hello,1)
(world,1)
...