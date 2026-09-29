from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy("status_bits_bucket", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("status_bits_bucket_per_prev_temp_c").desc())
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w9 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "*",
        F.first("pressure", ignorenulls=True).over(w).alias("first_known_pressure"),
        F.ntile(4).over(w_running).alias("status_bits_bucket"),
        F.lag("temp_c", 1).over(w3).alias("prev_temp_c"),
        F.lead("pressure", 1, 0).over(w_running).alias("next_pressure"),
        F.first("pressure", ignorenulls=True).over(w4).alias("first_known_pressure_2"),
    )
)

filtered = (
    with_windows
    .filter(F.col("device_code") == "DV-04")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "first_known_pressure",
        "status_bits_bucket",
        "next_pressure",
        "first_known_pressure_2",
        F.round((F.col("status_bits_bucket") / F.col("prev_temp_c")), 2).alias("status_bits_bucket_per_prev_temp_c"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "status_bits_bucket",
        "status_bits_bucket_per_prev_temp_c",
    )
)

metrics = (
    deduped
    .select(
        "*",
        F.max("status_bits_bucket").over(w6).alias("last4_max_status_bits_bucket"),
        F.avg("status_bits_bucket").over(w6).alias("last4_avg_status_bits_bucket"),
        F.count("status_bits_bucket").over(w6).alias("last4_count_status_bits_bucket"),
        F.min("status_bits_bucket").over(w6).alias("last4_min_status_bits_bucket"),
        F.dense_rank().over(w7).alias("status_bits_bucket_per_prev_temp_c_dense_rank"),
    )
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w8) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "status_bits_bucket",
        "last4_max_status_bits_bucket",
        "last4_avg_status_bits_bucket",
        "last4_min_status_bits_bucket",
        F.sum("session_start_flag").over(w9).alias("burst_id"),
    )
)

filtered_device_readings = (
    islands
    .filter((F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp")) & F.hour("reading_ts").between(9, 12))
    .withColumn("greatest_last4_min_status_bits_bucket_burst_id", F.greatest("last4_min_status_bits_bucket", "burst_id"))
)

result = (
    filtered_device_readings
    .withColumn("last4_min_status_bits_bucket_tier", F.when(F.col("last4_min_status_bits_bucket") >= 8.1, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["burst_id", "last4_min_status_bits_bucket_tier", "device_code"], ["burst_id", "last4_min_status_bits_bucket_tier"], ["burst_id"], []],
        "burst_id",
        "last4_min_status_bits_bucket_tier",
        "device_code",
    )
    .agg(
        F.max(F.when(F.col("device_code") == "DV-01", F.col("last4_avg_status_bits_bucket"))).alias("dv_01_max_last4_avg_status_bits_bucket"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.percentile_approx("last4_min_status_bits_bucket", [0.25, 0.5, 0.75]).alias("last4_min_status_bits_bucket_quartiles"),
        F.round(F.sum("last4_min_status_bits_bucket") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("last4_min_status_bits_bucket_per_dv_04"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
    )
    .orderBy("dv_03_count")
)
