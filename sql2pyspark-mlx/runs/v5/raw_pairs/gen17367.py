from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.col("reading_ts").cast("date").alias("reading_ts_day"))
)

with_prev = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "reading_ts",
        "firmware",
        "reading_ts_day",
        "prev_reading_ts",
        "is_new_burst",
    )
)

base_device_readings = (
    ranked
    .filter(F.month("reading_ts_day").isin([2, 6, 8]))
    .select(
        "id",
        "reading_ts",
        "firmware",
        "prev_reading_ts",
        "is_new_burst",
        F.col("reading_ts").cast("date").alias("reading_ts_day_2"),
    )
)

cleaned = (
    base_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .select("*", F.trim("firmware").alias("clean_firmware"))
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "reading_ts_day_2").alias("activity_month"),
        F.date_trunc("QUARTER", "reading_ts_day_2").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count("*").alias("event_count"),
        F.min("prev_reading_ts").alias("first_seen"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .orderBy(F.col("duration_min").desc_nulls_last())
)
