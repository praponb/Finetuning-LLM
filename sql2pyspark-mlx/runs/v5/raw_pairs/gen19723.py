from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("pressure") >= 1019, "HIGH").otherwise("LOW").alias("pressure_tier"),
        "device_code",
    )
    .agg(
        F.min("reading_ts").alias("started_at"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy(F.col("device_code").desc())
)
