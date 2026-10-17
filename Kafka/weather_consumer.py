import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()
if os.name == "nt":
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

SPARK_VERSION = "4.2.0"
HADOOP_VERSION = "3.5.0"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC = "weather.raw"
DLQ_TOPIC = "weather.dlq"
S3_BUCKET = os.environ["PARALITRACE_BUCKET"]
CLEAN_PATH = f"s3a://{S3_BUCKET}/clean/source=openmeteo"
DEAD_LETTER_PATH = f"s3a://{S3_BUCKET}/dead_letter/source=openmeteo"
CHECKPOINT = os.getenv("CHECKPOINT_ROOT", str(Path(__file__).parent / "checkpoints")) + "/weather_raw"
SCHEMA_JSON = (Path(__file__).resolve().parent.parent / "schemas" / "weather_raw.avsc").read_text(encoding="utf-8")
SPARK_TMP = os.getenv("SPARK_TMP", r"C:\spark-tmp" if os.name == "nt" else "/tmp/spark")

spark = (
    SparkSession.builder.appName("paralitrace-weather-stream").config("spark.jars.packages",f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION},"f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION},"f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",)
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", SPARK_TMP)
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={SPARK_TMP}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 20000)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka_df
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        from_avro(F.expr("substring(value, 6, length(value) - 5)"), SCHEMA_JSON).alias("f"),
    )
    .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "f.*")
)

deduped = (
    parsed
    .withWatermark("kafka_ts", "1 hour")
    .dropDuplicatesWithinWatermark(["point_key", "time_local", "source"])
)


typed = (
    deduped
    .withColumn("local_wall_ts", F.expr("try_to_timestamp(time_local, \"yyyy-MM-dd'T'HH:mm\")"))
    .withColumn("ts_utc", F.expr("timestamp_seconds(unix_timestamp(local_wall_ts) - utc_offset_seconds)"))
    .withColumn("ingested_at_ts", F.expr("try_cast(ingested_at AS TIMESTAMP)"))
)

rules = [
    (F.col("ts_utc").isNull(), "invalid_timestamp"),
    ((F.col("source") == "open_meteo_archive") & (F.col("ts_utc") > F.current_timestamp()), "archive_timestamp_in_future"),
    (F.col("wind_speed_10m") < 0,"negative_wind_speed_10m"),
    (F.col("wind_speed_100m") < 0,"negative_wind_speed_100m"),
    (~F.col("wind_direction_10m").between(0, 360),"invalid_wind_direction_10m"),
    (~F.col("wind_direction_100m").between(0, 360), "invalid_wind_direction_100m"),
    (~F.col("relative_humidity_2m").between(0, 100),"invalid_relative_humidity"),
    (F.col("precipitation") < 0,"negative_precipitation"),
    (F.col("boundary_layer_height") < 0,"negative_boundary_layer_height"),
    (~F.col("temperature_2m").between(-40, 60), "temperature_out_of_range"),
]

error_reason = F.concat_ws(";", *[F.when(cond, F.lit(name)) for cond, name in rules])
validated = typed.withColumn("error_reason", F.when(error_reason == "", F.lit(None)).otherwise(error_reason))

RAW_FIELDS = ["point_key", "requested_lat", "requested_lon", "grid_lat", "grid_lon", "elevation",
              "time_local", "timezone", "utc_offset_seconds", "wind_speed_10m", "wind_direction_10m",
              "wind_speed_100m", "wind_direction_100m", "temperature_2m", "relative_humidity_2m",
              "precipitation", "boundary_layer_height", "source", "request_window", "ingested_at"]


def write_batch(batch_df, batch_id):
    batch_df.persist()

    good = (
        batch_df.filter(F.col("error_reason").isNull())
        .select(
            "point_key", "requested_lat", "requested_lon", "grid_lat", "grid_lon", "elevation",
            "ts_utc", "time_local", "timezone",
            "wind_speed_10m", "wind_direction_10m", "wind_speed_100m", "wind_direction_100m",
            "temperature_2m", "relative_humidity_2m", "precipitation", "boundary_layer_height",
            "source", "request_window",
            F.col("ingested_at_ts").alias("ingested_at"),
            "kafka_partition", "kafka_offset",
            F.lit(batch_id).alias("batch_id"),
            F.date_format("ts_utc", "yyyy-MM-dd").alias("date"),
        )
    )

    bad = (
        batch_df.filter(F.col("error_reason").isNotNull())
        .select(
            *RAW_FIELDS, "error_reason", "kafka_key", "kafka_partition", "kafka_offset",
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