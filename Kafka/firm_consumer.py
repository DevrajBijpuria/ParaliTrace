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
SOURCE_TOPIC = "fires.raw"
DLQ_TOPIC = "fires.dlq"
S3_BUCKET = os.environ["PARALITRACE_BUCKET"]
CLEAN_PATH = f"s3a://{S3_BUCKET}/clean/source=firms"
DEAD_LETTER_PATH = f"s3a://{S3_BUCKET}/dead_letter/source=firms"
CHECKPOINT = os.getenv("CHECKPOINT_ROOT", str(Path(__file__).parent / "checkpoints")) + "/fires_raw"
SCHEMA_JSON = (Path(__file__).resolve().parent.parent / "schemas" / "fires_raw.avsc").read_text(encoding="utf-8")
SPARK_TMP = os.getenv("SPARK_TMP", r"C:\spark-tmp" if os.name == "nt" else "/tmp/spark")

spark=(SparkSession.builder.appName("fires-stream").config("spark.jars.packages",f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION},"f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION},"f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
          .config("spark.driver.memory", "4g")
          .config("spark.sql.shuffle.partitions", "4")
          .config("spark.sql.session.timeZone", "UTC")           
          .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
          .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
          .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
          .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
          .config("spark.hadoop.fs.s3a.fast.upload", "true")
          .config("spark.local.dir", SPARK_TMP)
          .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={SPARK_TMP}")
          .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

kafka_df=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP).option("subscribe",SOURCE_TOPIC).option("startingOffsets", "earliest").option("maxOffsetsPerTrigger",20000).option("failOnDataLoss","false").load())

parsed=(kafka_df.select(F.col("key").cast("string").alias("kafka_key"),F.col("partition").alias("kafka_partition"),F.col("offset").alias("kafka_offset"),F.col("timestamp").alias("kafka_ts"), from_avro(F.expr("substring(value, 6, length(value) - 5)"),SCHEMA_JSON).alias("f"),)
        .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "f.*"))

dedup=parsed.withWatermark("kafka_ts","1 hour").dropDuplicatesWithinWatermark(["latitude", "longitude", "acq_date", "acq_time", "satellite"])

typed=(dedup.withColumn("lat",F.expr("try_cast(latitude AS DOUBLE)"))
            .withColumn("lon",F.expr("try_cast(longitude AS DOUBLE)"))
            .withColumn("bright_ti4_d",F.expr("try_cast(bright_ti4 AS DOUBLE)"))
            .withColumn("bright_ti5_d",F.expr("try_cast(bright_ti5 AS DOUBLE)"))
            .withColumn("scan_d", F.expr("try_cast(scan AS DOUBLE)"))
            .withColumn("track_d", F.expr("try_cast(track AS DOUBLE)"))
            .withColumn("frp_d", F.expr("try_cast(frp AS DOUBLE)"))
            .withColumn("type_i", F.expr("try_cast(type AS INT)"))
            .withColumn("acq_ts_utc",F.expr("try_to_timestamp(concat(acq_date, ' ', lpad(acq_time, 4, '0')), 'yyyy-MM-dd HHmm')"),)
            .withColumn("satellite_std",
                F.when(F.col("satellite").isin("SNPP", "N"), "SNPP")
                 .when(F.col("satellite").isin("N20", "NOAA20"), "NOAA20")
                 .when(F.col("satellite").isin("N21", "NOAA21"), "NOAA21"),)
            .withColumn("processing_level",F.when(F.upper(F.col("version")).contains("NRT"), "NRT").otherwise("SP"),)
            .withColumn("ingested_at_ts", F.expr("try_to_timestamp(ingested_at)")))

rules = [
    (F.col("lat").isNull() | ~F.col("lat").between(-90, 90),"invalid_latitude"),
    (F.col("lon").isNull() | ~F.col("lon").between(-180, 180),"invalid_longitude"),
    (F.col("acq_ts_utc").isNull(), "invalid_acq_timestamp"),
    (F.col("acq_ts_utc") > F.current_timestamp(),"acq_timestamp_in_future"),
    (F.col("satellite_std").isNull(), "unknown_satellite"),
    (F.col("frp_d").isNull() | (F.col("frp_d") < 0),"invalid_frp"),
    (~F.coalesce(F.col("confidence").isin("l", "n", "h"), F.lit(False)), "invalid_confidence"),
    (~F.coalesce(F.col("daynight").isin("D", "N"), F.lit(False)),        "invalid_daynight"),
]

error_reason = F.concat_ws(";", *[F.when(cond, F.lit(name)) for cond, name in rules])

validated = typed.withColumn("error_reason", F.when(error_reason == "", F.lit(None)).otherwise(error_reason))

RAW_FIELDS = ["latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time",
              "satellite", "instrument", "confidence", "version", "bright_ti5", "frp",
              "daynight", "type", "source", "source_file", "ingested_at"]


def write_batch(batch_df, batch_id):
    batch_df.persist()

    good = (
        batch_df.filter(F.col("error_reason").isNull())
        .select(
            F.col("lat").alias("latitude"),
            F.col("lon").alias("longitude"),
            F.col("bright_ti4_d").alias("bright_ti4"),
            F.col("bright_ti5_d").alias("bright_ti5"),
            F.col("scan_d").alias("scan"),
            F.col("track_d").alias("track"),
            F.col("frp_d").alias("frp"),
            "acq_ts_utc",
            F.col("satellite").alias("satellite_raw"),
            "satellite_std",
            "instrument",
            "confidence",
            "version",
            "processing_level",
            "daynight",
            F.col("type_i").alias("type"),
            "source",
            "source_file",
            F.col("ingested_at_ts").alias("ingested_at"),
            "kafka_partition",
            "kafka_offset",
            F.lit(batch_id).alias("batch_id"),
            F.date_format("acq_ts_utc", "yyyy-MM-dd").alias("date"),
            F.date_format("acq_ts_utc", "HH").alias("hour"),
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