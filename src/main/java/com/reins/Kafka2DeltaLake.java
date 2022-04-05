package com.reins;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Kafka2DeltaLake {
    private static boolean debug = false;

    private static String kafkaIp = "10.0.0.203:9092";
    private static String topic = "taxi_log_1";

    private static String tableName = "taxi_log";
    private static String storagePath = String.format("hdfs://10.0.0.203:9000/delta/%s", topic);

    private static String warehouseDir = "hdfs://10.0.0.203:9000/delta/warehouse/";

    public static void main(String[] args) {
        if (debug) {
            test();
            return;
        }
        SparkSession spark = SparkSession.builder()
                .config("spark.master", "local")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                        "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/delta_metastore")
                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                        "com.mysql.cj.jdbc.Driver")
                .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "zzt")
                .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "Zzt19980924x")
                .config("spark.hadoop.datanucleus.autoCreateSchema", true)
                .config("spark.hadoop.datanucleus.autoCreateTables", true)
                .config("spark.hadoop.datanucleus.fixedDatastore", false)
                .config("spark.hadoop.datanucleus.readOnlyDatastore", false)
                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                .config("spark.hadoop.hive.metastore.schema.verification", false)
                .config("spark.hadoop.hive.metastore.schema.verification.record.version",
                        false)
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        StructType schema = new StructType()
                .add("uuid", DataTypes.StringType)
                .add("taxiId", DataTypes.LongType)
                .add("tripId", DataTypes.StringType)
                .add("ts", DataTypes.LongType)
                .add("longitude", DataTypes.DoubleType)
                .add("latitude", DataTypes.DoubleType)
                .add("speed", DataTypes.DoubleType);

        DataStreamReader dataStreamReader = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", 100000)
                .option("failOnDataLoss", false);
        Dataset<Row> df = dataStreamReader.load();
        df.printSchema();

        Dataset<Row> logStringDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        Dataset<Row> logDf = logStringDf.select(from_json(col("value"), schema).as("data")).select("data.*");
        try {
            StreamingQuery query = logDf.writeStream()
                    // .queryName("query log from kafka to delta")
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", "/home/reins/zzt/code/spark-to-delta/ckpt")
                    .toTable(topic);
            // .start(storagePath);
            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void test() {
        SparkSession spark = SparkSession.builder()
                .config("spark.master", "local")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        Dataset<Long> data = spark.range(0, 5);
        data.write().format("delta").save("/home/reins/zzt/code/spark-to-delta/tmp/delta-table");

        log.warn("write finish");

        Dataset<Row> df = spark.read().format("delta").load("/home/reins/zzt/code/spark-to-delta/tmp/delta-table");
        df.show();

        spark.close();
    }
}
