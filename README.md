# -Hadoop-
1）Flume、Hadoop、Hbase、Hive、Oozie、Sqoop、离线数据分析，SpringMVC，Highchat<br>
2）Flume+Hadoop+Hbase+SpringMVC+MyBatis+MySQL+Highcharts实现的电商离线数据分析<br>
3）日志收集系统、日志分析、数据展示设计<br>

本项目主要以分析七个模块的数据，分别为用户基本信息分析、操作系统分析、地域信息分析、用户浏览深度分析、外链数据分析、订单信息分析以及事件分析。<br>
那么针对不同的分析模块，我们又不同的用户数据需求，所以我们在日志收集tracker模块中提供不同的客户端来收集不同的数据。在日志分析transformer模块中采用hive+mr两种方式进行数据分析。在结果展示dataapi模块中进行分析结果的api提供以及结果图表展示。

