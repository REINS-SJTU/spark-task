SPARK_SUBMIT=/home/reins/zzt/spark-3.1.3-bin-hadoop2.7/bin/spark-submit
JAR=hdfs://10.0.0.203:9000/spark-to-delta-1.0-SNAPSHOT.jar
CLASS=com.reins.Kafka2DeltaLake
TABLE_NAME=taxi_2

$SPARK_SUBMIT \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-core_2.12:1.0.0,mysql:mysql-connector-java:8.0.16 \
--class $CLASS \
--master spark://reins-PowerEdge-R740-0:7077 \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 10 \
$JAR
# $TABLE_NAME
