from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.orderBy(F.col("status_bits").desc())
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w4 = Window.partitionBy("device_code")
w_running = Window.partitionBy("device_code").orderBy("status_bits_cume_dist", "id")
w6 = Window.partitionBy("device_code").orderBy("status_bits_cume_dist", "id").rowsBetween(-3, -1)
w_part = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.orderBy(F.col("status_bits_max_2h").asc_nulls_last())
w9 = Window.orderBy(F.col("status_bits_cume_dist").asc_nulls_first())
w10 = Window.orderBy(F.col("corr_status_bits_pressure_int").desc())
w11 = Window.orderBy(F.col("row_seq").desc())
w12 = Window.orderBy(F.col("next_status_bits_cume_dist").desc())

with_windows = (
    df_device_readings
    .filter(F.col("humidity") < 67)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("status_bits_cume_dist"),
        F.rank().over(w3).alias("status_bits_rank"),
    )
)

with_windows_device_readings = (
    with_windows
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "row_seq",
        "status_bits_cume_dist",
        F.corr("status_bits", "pressure").over(w4).alias("corr_status_bits_pressure"),
        F.ntile(10).over(w_running).alias("status_bits_cume_dist_bucket"),
        F.lead("status_bits_cume_dist", 1).over(w_running).alias("next_status_bits_cume_dist"),
        F.avg("humidity").over(w6).alias("humidity_avg_3_prior"),
    )
)

remaining_keys = (
    with_windows_device_readings
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("humidity") > 63)
        .select("id")
    )
)

kept_rows = (
    with_windows_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("status_bits_cume_dist_bucket").isNotNull())
    .withColumn("status_bits_cume_dist_bucket_scaled", F.round((F.col("status_bits_cume_dist_bucket") * 1.15), 2))
    .withColumn("corr_status_bits_pressure_int", F.col("corr_status_bits_pressure").cast("int"))
)

metrics = (
    filtered
    .select(
        "*",
        F.max("status_bits").over(w_part).alias("status_bits_max_2h"),
        F.avg("row_seq").over(w_part).alias("row_seq_avg_2h"),
    )
)

base = (
    metrics
    .filter(F.col("status_bits_cume_dist").between(0.25, 0.27))
    .select("*", F.round("row_seq_avg_2h", 2).alias("row_seq_avg_2h_rounded"))
)

metrics_device_readings = (
    base
    .withColumn("status_bits_max_2h_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("status_bits_cume_dist_cume_dist", F.cume_dist().over(w9))
    .withColumn("corr_status_bits_pressure_int_percent_rank", F.percent_rank().over(w10))
    .withColumn("row_seq_percent_rank", F.percent_rank().over(w11))
    .withColumn("next_status_bits_cume_dist_rank", F.rank().over(w12))
)

grouped = (
    metrics_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("row_seq_percent_rank", [0.25, 0.5, 0.75]).alias("row_seq_percent_rank_quartiles"),
        F.percentile_approx("row_seq_avg_2h", [0.25, 0.5, 0.75]).alias("row_seq_avg_2h_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "hour_window",
        "row_seq_percent_rank_quartiles",
        "row_seq_avg_2h_quartiles",
        "distinct_ids",
    )
)
