from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("greatest_status_bits_temp_c").desc())
w2 = Window.orderBy(F.col("greatest_status_bits_temp_c").desc_nulls_first())
w3 = Window.orderBy(F.col("greatest_status_bits_temp_c_percent_rank_2").asc_nulls_last())
w4 = Window.orderBy("greatest_status_bits_temp_c_percent_rank_2")
w5 = Window.orderBy(F.col("greatest_status_bits_temp_c_percent_rank").desc())

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.greatest("status_bits", "temp_c").alias("greatest_status_bits_temp_c"),
    )
)

remaining_keys = (
    base
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "2.0.1")
        .select("device_code")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("device_code") == "DV-04")
    .select(
        "greatest_status_bits_temp_c",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

with_windows = (
    valid
    .select(
        "greatest_status_bits_temp_c",
        "reading_ts_day",
        F.percent_rank().over(w).alias("greatest_status_bits_temp_c_percent_rank"),
        F.percent_rank().over(w2).alias("greatest_status_bits_temp_c_percent_rank_2"),
        F.percent_rank().over(w).alias("greatest_status_bits_temp_c_percent_rank_3"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w3).alias("greatest_status_bits_temp_c_percent_rank_2_percent_rank"),
        F.dense_rank().over(w4).alias("greatest_status_bits_temp_c_percent_rank_2_dense_rank"),
        F.cume_dist().over(w5).alias("greatest_status_bits_temp_c_percent_rank_cume_dist"),
        F.cume_dist().over(w4).alias("greatest_status_bits_temp_c_percent_rank_2_cume_dist"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("greatest_status_bits_temp_c_percent_rank_2") >= 0.75, "PREMIUM").otherwise("STANDARD").alias("greatest_status_bits_temp_c_percent_rank_2_tier"),
        F.date_trunc("month", "reading_ts_day").alias("activity_month"),
    )
    .agg(
        F.sum("greatest_status_bits_temp_c").alias("total_greatest_status_bits_temp_c"),
        F.round(F.avg("greatest_status_bits_temp_c"), 2).alias("avg_greatest_status_bits_temp_c"),
        F.round(F.avg("greatest_status_bits_temp_c"), 2).alias("avg_greatest_status_bits_temp_c_2"),
        F.max("greatest_status_bits_temp_c_percent_rank_2_dense_rank").alias("max_greatest_status_bits_temp_c_percent_rank_2_dense_rank"),
        F.count("*").alias("row_count"),
        F.percentile_approx("greatest_status_bits_temp_c_percent_rank_cume_dist", 0.9).alias("greatest_status_bits_temp_c_percent_rank_cume_dist_p90"),
    )
)

result = (
    stats
    .select(
        "greatest_status_bits_temp_c_percent_rank_2_tier",
        "activity_month",
        "avg_greatest_status_bits_temp_c",
        "avg_greatest_status_bits_temp_c_2",
        "max_greatest_status_bits_temp_c_percent_rank_2_dense_rank",
        "greatest_status_bits_temp_c_percent_rank_cume_dist_p90",
    )
)
