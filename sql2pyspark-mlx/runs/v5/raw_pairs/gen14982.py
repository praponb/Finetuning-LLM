from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .select("*", F.col("status_bits").cast("string").alias("status_bits_num"))
)

with_prev = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 74, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("humidity_per_dv_04"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
    )
)
