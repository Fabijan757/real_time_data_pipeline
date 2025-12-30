from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType
import mysql.connector


mysql_host = "localhost"
mysql_user = "root"
mysql_password = "NovaLozinka"
db_name = "LiveDB"
table_name = "events_stream"



def init_mysql():
    conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item VARCHAR(50),
            boja VARCHAR(50),
            akcija VARCHAR(50),
            event_time TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

init_mysql()



spark = SparkSession.builder.appName("LiveKafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "live_events")
        .option("startingOffsets", "latest")
        .load()
)

schema = StructType([
    StructField("item", StringType(), True),
    StructField("boja", StringType(), True),
    StructField("akcija", StringType(), True),
    StructField("timestamp", LongType(), True),
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")

parsed_df = json_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

final_df = (
    parsed_df
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))
    .drop("timestamp")
)



def write_to_mysql(batch_df, batch_id):
    (
        batch_df.write
            .format("jdbc")
            .mode("append")
            .option("url", f"jdbc:mysql://{mysql_host}:3306/{db_name}")
            .option("dbtable", table_name)
            .option("user", mysql_user)
            .option("password", mysql_password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .save()
    )
    print(f"Batch {batch_id} spremljen.")

query = (
    final_df.writeStream
        .foreachBatch(write_to_mysql)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/live_kafka_stream")
        .start()
)

query.awaitTermination()
