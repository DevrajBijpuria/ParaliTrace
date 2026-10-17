from pyspark.sql.avro.functions import from_avro
from pyspark.sql import SparkSession
from pyspark.sql import functions as  F 
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()
if os.name == "nt":
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]
SPARK_VERSION = "4.2.0"          
HADOOP_VERSION = "3.5.0"         
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")  
SOURCE_TOPIC = "aq.raw"
DLQ_TOPIC = "aq.dlq"
S3_BUCKET = os.environ["PARALITRACE_BUCKET"]
CLEAN_PATH = f"s3a://{S3_BUCKET}/clean/source=openaq"
DEAD_LETTER_PATH = f"s3a://{S3_BUCKET}/dead_letter/source=openaq"
CHECKPOINT = os.getenv("CHECKPOINT_ROOT", str(Path(__file__).parent / "checkpoints")) + "/aq_raw"
SCHEMA_JSON = (Path(__file__).resolve().parent.parent / "schemas" / "aq_raw.avsc").read_text(encoding="utf-8")

spark=(SparkSession.builder.appName("aq-stream").config("spark.jars.packages",f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION},"f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION},"f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
          .config("spark.driver.memory", "2g")
          .config("spark.sql.shuffle.partitions", "4")
          .config("spark.sql.session.timeZone", "UTC")           
          .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
          .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
          .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
          .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
          .config("spark.hadoop.fs.s3a.fast.upload", "true")
          .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

kafka_df=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP).option("subscribe",SOURCE_TOPIC).option("startingOffsets", "earliest").option("maxOffsetsPerTrigger",20000).option("failOnDataLoss","false").load())

parsed=(kafka_df.select(F.col("key").cast("string").alias("kafka_key"),F.col("partition").alias("kafka_partition"),F.col("offset").alias("kafka_offset"),F.col("timestamp").alias("kafka_ts"), from_avro(F.expr("substring(value, 6, length(value) - 5)"),SCHEMA_JSON).alias("f"),)
        .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "f.*"))

dedup=parsed.withWatermark("kafka_ts","1 hour").dropDuplicatesWithinWatermark(["sensor_id","datetime_from_utc"])

typed=(dedup.withColumn("period_start_utc", F.expr("try_cast(datetime_from_utc AS TIMESTAMP)"))
            .withColumn("period_end_utc", F.expr("try_cast(datetime_to_utc AS TIMESTAMP)"))
            .withColumn("period_start_minute", F.minute("period_start_utc"))
            .withColumn("is_canonical_hour", F.col("period_start_minute") == 30)  
            .withColumn("ingested_at_ts", F.expr("try_cast(ingested_at AS TIMESTAMP)")))

rules = [
    (F.col("value").isNull(),"missing_value"),
    (F.col("value")<0,"negative_value"),
    (F.col("value")>2000, "value_above_physical_max"),
    (F.coalesce(F.col("parameter")!="pm25",F.lit(True)),"unexpected_parameter"),
    (F.col("period_start_utc").isNull(), "invalid_period_start"),
    (F.col("period_end_utc").isNull() | (F.col("period_end_utc") <= F.col("period_start_utc")), "invalid_period_end"),
    (F.col("period_start_utc") > F.current_timestamp(),"period_in_future")
]

error_reason = F.concat_ws(";", *[F.when(cond, F.lit(name)) for cond, name in rules])

validated = typed.withColumn("error_reason", F.when(error_reason == "", F.lit(None)).otherwise(error_reason))

RAW_FIELDS = ["station_key", "location_id", "sensor_id", "parameter", "units", "value",
              "datetime_from_utc", "datetime_to_utc", "observed_count", "expected_count",
              "percent_coverage", "has_flags", "source", "request_window", "ingested_at"]


def write_batch(batch_df, batch_id):
    batch_df.persist()

    good = (
        batch_df.filter(F.col("error_reason").isNull())
        .select(
            "station_key",
            "location_id",
            "sensor_id",
            "parameter",
            "units",
            F.col("value").alias("pm25"),
            "period_start_utc",
            "period_end_utc",
            "period_start_minute",
            "is_canonical_hour",
            "observed_count",
            "expected_count",
            "percent_coverage",
            "has_flags",
            "source",
            "request_window",
            F.col("ingested_at_ts").alias("ingested_at"),
            "kafka_partition",
            "kafka_offset",
            F.lit(batch_id).alias("batch_id"),
            F.date_format("period_start_utc", "yyyy-MM-dd").alias("date"),
            F.date_format("period_start_utc", "HH").alias("hour"),
        )
    )

    bad = (
        batch_df.filter(F.col("error_reason").isNotNull())
        .select(
            *RAW_FIELDS,                       
            "error_reason",
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("rejected_at"),
            F.current_date().cast("string").alias("rejected_date"),
        )
    )

    good.repartition("date").write.mode("append").partitionBy("date").parquet(CLEAN_PATH)

    if not bad.isEmpty():
        bad.write.mode("append").partitionBy("rejected_date").json(DEAD_LETTER_PATH)
        (
            bad.select(
                F.col("kafka_key").alias("key"),
                F.to_json(F.struct(*[c for c in bad.columns if c != "rejected_date"])).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", DLQ_TOPIC)
            .save()
        )

    print(f"batch {batch_id}: good={good.count()} bad={bad.count()}")
    batch_df.unpersist()


query = (
    validated.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()