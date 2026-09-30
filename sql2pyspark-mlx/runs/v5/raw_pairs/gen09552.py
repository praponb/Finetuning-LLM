from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())
w_part = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .select(
        "*",
        F.sum("status_bits").over(w_part).alias("last4_sum_status_bits"),
        F.min("status_bits").over(w_part).alias("last4_min_status_bits"),
        F.avg("status_bits").over(w_part).alias("last4_avg_status_bits"),
        F.ntile(5).over(w3).alias("humidity_bucket"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w4) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w5))

flagged_2 = (
    sessionized
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w4).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    flagged_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "last4_sum_status_bits",
        "is_new_burst",
        F.sum("is_new_session").over(w5).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count("*").alias("event_count"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
)
