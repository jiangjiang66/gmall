# gmall
# gmall
使用mysql,canal,scala,sqoop,flume,kafka,hdfs,,hbase,zookeeper,Spark,ELK(es, logstash, kibana),redis,SpringBoot,echarts等技术实现用户行为数据、业务数据的离线分析和实时分析
实时数据分析T+0

用户行为数据
1.实现（gmall-mock子模块）生成启动日志模拟数据，事件日志模拟数据
2.将日志json字符（gmall-logger子模块）实时导入kafka，借助sparkstream（gmall-realtime子模块）获取分析kafka数据，将结果存到es,redis,hbase
3.使用kibana工具分析es数据，形成图表，使用SpringBoot（gmall-publisher子模块）查询得到es数据，使用RestFul风格的接口对外开放，将获取的数据使用echarts（dw-chart子模块）进行展示
4.使用SpringBoot(phoenix,mybatis,jdbc gmall-publisher子模块)查询获取到hbase数据，使用RestFul风格的接口对外开放，将获取的数据使用echarts进行展示
5.使用SpringBoot(redisTemplate gmall-realtime子模块)查询获取到redis数据，辅助hbase、es分析

业务数据（mysql）
1.在mysql实现生成模拟业务数据
2.使用canal（行级）监听mysql数据的变化
3.使用SpringBoot（gmall-canal子模块）查询得到canal里边的数据，借助sparkstream获取分析canal数据,将结果存到es,redis,hbase


离线数据分析T+0
用户行为日志
1.生成模拟数据，将数据存储到日志文件
2.使用flume监听获取日志变化数据，并将数据发送到kafka,使用kafka将数据发送到hdfs
3.使用hive分层建立ODS(原始数据层)、dwd(明细层)、ADS(业务层)等多层数仓数据
4.使用SparkSql分析hive数据，将结果数据导入到mysql
5.使用tomcat查询mysql数据，echarts前端展示

业务数据
1.在mysql实现生成模拟业务数据
2.将数据通过sqoop导入hdfs
3.使用hive分层建立ODS(原始数据层)、dwd(明细层)、ADS(业务层)等多层数仓数据
4.使用SparkSql分析hive数据，将结果数据导入到mysql
5.使用tomcat查询mysql数据，echarts前端展示

创建azkaban定时调度任务，在每一天的凌晨将数据分析生成到mysql

