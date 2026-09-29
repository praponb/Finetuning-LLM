from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("session_no").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("status_bits_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("activity_day").orderBy(F.col("dv_01_count").desc())

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

enriched = (
    sessions_device_readings
    .filter(F.col("pressure").isNull())
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "session_no",
        F.lead("humidity", 1).over(w).alias("next_humidity"),
        F.ntile(4).over(w3).alias("status_bits_bucket"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    latest
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "session_no",
        "status_bits_bucket",
        F.substring("device_code", 1, 4).alias("device_code_prefix"),
    )
)

metrics = (
    base
    .withColumn("first_known_status_bits_bucket", F.first("status_bits_bucket", ignorenulls=True).over(w5))
    .withColumn("seq_no", F.row_number().over(w3))
)

stats = (
    metrics
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("first_known_status_bits_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("first_known_status_bits_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("first_known_status_bits_bucket"))).alias("dv_01_avg_first_known_status_bits_bucket"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.avg("seq_no").alias("avg_seq_no"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .filter(F.col("dv_01_count") > 1)
    .withColumn("dv_01_count_dense_rank", F.dense_rank().over(w6))
)

result = (
    stats
    .select(
        "activity_day",
        "first_known_status_bits_bucket_tier",
        "distinct_ids",
        "dv_01_count",
        "dv_01_avg_first_known_status_bits_bucket",
        "avg_status_bits",
        "avg_seq_no",
        "num_device_codes",
        "dv_01_count_dense_rank",
    )
    .orderBy(F.col("first_known_status_bits_bucket_tier").desc_nulls_first())
)
