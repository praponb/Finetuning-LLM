from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("prev_reading_ts").asc_nulls_last())
w4 = Window.partitionBy("device_code").orderBy("pressure")
w_part = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc())
w6 = Window.partitionBy("device_code")

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

valid = (
    sessionized
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("firmware").isin(["1.0.3", "2.0.1"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "is_new_burst",
        (F.col("status_bits") * F.col("is_new_burst")).alias("status_bits_times_is_new_burst"),
        F.when(F.col("firmware").isin(["1.0.3", "2.0.1"]), "Priority").otherwise("Other").alias("firmware_group"),
    )
)

with_prev = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

ranked = (
    sessionized_device_readings
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("pressure_rank", F.rank().over(w4))
    .withColumn("new_group_flag_percent_rank", F.percent_rank().over(w_part))
    .withColumn("corr_pressure_status_bits", F.corr("pressure", "status_bits").over(w6))
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("status_bits") >= 13, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
    )
    .orderBy(F.col("avg_pressure").asc_nulls_first())
)
