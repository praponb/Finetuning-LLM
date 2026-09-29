from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("running_min_prev_temp_c").desc(), "id")
w4 = Window.orderBy("status_bits", "id")
w5 = Window.orderBy("running_min_prev_temp_c", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("row_seq", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("running_sum_prev_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("prev_temp_c").desc())
w9 = Window.orderBy(F.col("temp_c").desc(), "id")
w10 = Window.orderBy("prev_temp_c", "id").rowsBetween(-5, Window.currentRow)
w11 = Window.orderBy("next_humidity", "id").rowsBetween(-6, -1)

enriched = (
    df_device_readings
    .select(
        "id",
        "temp_c",
        "status_bits",
        F.lead("humidity").over(w_recent).alias("next_humidity"),
        F.lag("temp_c").over(w_recent).alias("prev_temp_c"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

scored = (
    enriched
    .filter(F.col("temp_c").between(25, 37))
    .withColumn("running_avg_prev_temp_c", F.avg("prev_temp_c").over(w2))
    .withColumn("running_min_prev_temp_c", F.min("prev_temp_c").over(w2))
    .withColumn("running_sum_prev_temp_c", F.sum("prev_temp_c").over(w2))
)

with_windows = (
    scored
    .select(
        "*",
        F.ntile(10).over(w3).alias("running_min_prev_temp_c_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

scored_device_readings = (
    with_windows
    .withColumn("running_sum_prev_temp_c_avg_6", F.avg("running_sum_prev_temp_c").over(w5))
    .withColumn("running_min_prev_temp_c_bucket_avg_5_prior", F.avg("running_min_prev_temp_c_bucket").over(w6))
)

with_windows_device_readings = (
    scored_device_readings
    .select(
        "id",
        "next_humidity",
        "prev_temp_c",
        "running_min_prev_temp_c",
        "running_sum_prev_temp_c",
        "event_seq",
        "running_sum_prev_temp_c_avg_6",
        "running_min_prev_temp_c_bucket_avg_5_prior",
        F.first("running_sum_prev_temp_c", ignorenulls=True).over(w7).alias("first_known_running_sum_prev_temp_c"),
        F.rank().over(w_hist).alias("prev_temp_c_rank"),
        F.row_number().over(w9).alias("event_seq_2"),
        F.avg("running_sum_prev_temp_c_avg_6").over(w10).alias("running_sum_prev_temp_c_avg_6_avg_5"),
        F.avg("status_bits").over(w11).alias("status_bits_avg_6_prior"),
    )
)

base = (
    with_windows_device_readings
    .filter(F.col("running_sum_prev_temp_c") >= 90)
    .select(
        "id",
        "prev_temp_c",
        "running_min_prev_temp_c",
        "running_sum_prev_temp_c",
        "event_seq",
        "running_sum_prev_temp_c_avg_6",
        "running_min_prev_temp_c_bucket_avg_5_prior",
        "prev_temp_c_rank",
        "event_seq_2",
        "running_sum_prev_temp_c_avg_6_avg_5",
        "status_bits_avg_6_prior",
        F.round((F.col("prev_temp_c") * 0.9), 2).alias("adjusted_prev_temp_c"),
    )
)

totals = (
    base
    .groupBy(
        F.when(F.col("status_bits_avg_6_prior") >= 9, "HIGH").otherwise("LOW").alias("status_bits_avg_6_prior_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("event_seq_2", 0.95).alias("event_seq_2_p95"),
        F.percentile_approx("prev_temp_c_rank", 0.9).alias("prev_temp_c_rank_p90"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select("status_bits_avg_6_prior_tier", "num_rows", "prev_temp_c_rank_p90", "cnt")
)
