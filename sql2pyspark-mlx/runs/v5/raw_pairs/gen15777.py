from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("burst_id")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy("burst_id_dense_rank_scaled", "id")
w7 = Window.orderBy(F.col("status_bits").desc(), "id")

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

scored = (
    islands
    .select(
        "id",
        "temp_c",
        "status_bits",
        "burst_id",
        F.dense_rank().over(w_running).alias("burst_id_dense_rank"),
        F.lag("humidity", 1).over(w4).alias("prev_humidity"),
    )
)

cleaned = (
    scored
    .filter(F.col("prev_humidity") < 54)
    .select(
        "id",
        "temp_c",
        "status_bits",
        "prev_humidity",
        F.round((F.col("burst_id_dense_rank") * 0.85), 2).alias("burst_id_dense_rank_scaled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("burst_id_dense_rank_scaled").between(46, 51))
    .select(
        "id",
        "prev_humidity",
        "burst_id_dense_rank_scaled",
        F.avg("prev_humidity").over(w5).alias("prev_humidity_avg_2"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.ntile(4).over(w7).alias("status_bits_bucket"),
    )
)

base = (
    with_windows
    .filter(F.col("prev_humidity_avg_2").between(39, 49))
    .select(
        "id",
        "prev_humidity",
        "event_seq",
        "status_bits_bucket",
        F.coalesce("burst_id_dense_rank_scaled", F.lit(0)).alias("burst_id_dense_rank_scaled_filled"),
        (F.col("prev_humidity") - F.col("event_seq")).alias("prev_humidity_minus_event_seq"),
    )
)

result = (
    base
    .withColumn("prev_humidity_minus_event_seq_tier", F.when(F.col("prev_humidity_minus_event_seq") >= 65, "HIGH").otherwise("LOW"))
    .withColumn("status_bits_bucket_tier", F.when(F.col("status_bits_bucket") >= 6, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_humidity_minus_event_seq_tier", "status_bits_bucket_tier"], ["prev_humidity_minus_event_seq_tier"], []],
        "prev_humidity_minus_event_seq_tier",
        "status_bits_bucket_tier",
    )
    .agg(
        F.percentile_approx("prev_humidity", 0.95).alias("prev_humidity_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("status_bits_bucket", [0.25, 0.5, 0.75]).alias("status_bits_bucket_quartiles"),
        F.grouping("prev_humidity_minus_event_seq_tier").alias("grouping_id"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
