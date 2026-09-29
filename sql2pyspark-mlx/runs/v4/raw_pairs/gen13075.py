from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_bits", "id")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("pressure", "id")
w_hist = Window.orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("session_id").desc())

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

scored = (
    sessions_device_readings
    .withColumn("status_bits_bucket", F.ntile(3).over(w3))
    .withColumn("is_new_burst_count_2h", F.count("is_new_burst").over(w4))
    .withColumn("pressure_bucket", F.ntile(4).over(w_grp))
    .withColumn("running_session_id", F.sum("session_id").over(w_hist))
    .withColumn("session_id_percent_rank", F.percent_rank().over(w7))
)

with_gaps_2 = (
    scored
    .withColumn("is_new_burst_2", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps_2.withColumn("burst_id", F.sum("is_new_burst_2").over(w2))

base = (
    islands
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 12:00:00").cast("timestamp")) & (F.col("session_id") > 4))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "is_new_burst",
        "pressure_bucket",
        "running_session_id",
        "session_id_percent_rank",
        "is_new_burst_2",
        "burst_id",
        (F.col("status_bits_bucket") + F.col("is_new_burst_2")).alias("status_bits_bucket_plus_is_new_burst_2"),
        F.round((F.col("status_bits") * 1.07), 2).alias("status_bits_adjusted"),
    )
)

cleaned = (
    base
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("is_new_burst_2") <= 0))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "is_new_burst",
        "pressure_bucket",
        "running_session_id",
        "is_new_burst_2",
        "burst_id",
        "status_bits_bucket_plus_is_new_burst_2",
        "status_bits_adjusted",
        F.when(F.col("running_session_id") >= 36, "High").when(F.col("running_session_id") >= 9, "Medium").otherwise("Low").alias("running_session_id_tier"),
        (F.col("status_bits") - F.col("humidity")).alias("status_bits_minus_humidity"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        "running_session_id",
        "burst_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("reading_ts").alias("first_seen"),
        F.min("reading_ts").alias("started_at"),
    )
    .orderBy(F.col("burst_id_tier").desc())
)
