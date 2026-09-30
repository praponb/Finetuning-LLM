from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity_times_session_no", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("session_id").desc())
w5 = Window.partitionBy("device_code").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

filtered = (
    islands
    .filter(F.col("reading_ts") < "2024-11-01")
    .withColumn("clean_device_code", F.trim("device_code"))
    .withColumn("clean_firmware", F.trim("firmware"))
)

filtered_device_readings = (
    filtered
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("humidity_times_session_no", (F.col("humidity") * F.col("session_no")))
)

with_gaps = (
    filtered_device_readings
    .withColumn("session_start_flag_2", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("session_start_flag_2").over(w2))

latest = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .withColumn("session_id_dense_rank", F.dense_rank().over(w4))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w5))
    .withColumn("session_start_flag_2_count_30m", F.count("session_start_flag_2").over(w6))
)

daily = (
    with_windows
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("humidity") >= 63, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.max("session_no").alias("max_session_no"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
        F.max("reading_ts").alias("ended_at"),
    )
)

result = (
    daily
    .select("activity_day", "humidity_tier", "max_session_no", "session_id_quartiles")
    .orderBy(F.col("max_session_no").asc_nulls_first())
)
