from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c_filled", "id")
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy("temp_c_filled", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("temp_c_per_temp_c_filled_sum_15m", "id")
w_recent = Window.orderBy("temp_c_per_temp_c_filled_count_15m", "id").rowsBetween(-5, -1)

valid = (
    df_device_readings
    .filter(F.col("temp_c") >= 31)
    .withColumn("temp_c_filled", F.coalesce("temp_c", F.lit(0)))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "reading_ts", "temp_c", "humidity", "firmware", "temp_c_filled")
)

valid_device_readings = (
    latest
    .filter(F.col("firmware").isin(["2.0.1", "1.0.3", "1.1.0"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "temp_c_filled",
        F.when(F.col("temp_c") >= 38.2, "High").when(F.col("temp_c") >= 18.2, "Medium").otherwise("Low").alias("temp_c_band"),
        (F.col("temp_c") / F.nullif("temp_c_filled", F.lit(0))).alias("temp_c_per_temp_c_filled"),
    )
)

with_windows = (
    valid_device_readings
    .withColumn("temp_c_per_temp_c_filled_sum_15m", F.sum("temp_c_per_temp_c_filled").over(w3))
    .withColumn("temp_c_per_temp_c_filled_count_15m", F.count("temp_c_per_temp_c_filled").over(w3))
)

metrics = (
    with_windows
    .filter(F.col("temp_c") < 30)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "temp_c_filled",
        "temp_c_band",
        "temp_c_per_temp_c_filled",
        "temp_c_per_temp_c_filled_sum_15m",
        F.avg("temp_c_per_temp_c_filled_count_15m").over(w4).alias("temp_c_per_temp_c_filled_count_15m_avg_6_prior"),
        F.lead("temp_c_per_temp_c_filled_sum_15m", 2).over(w5).alias("next_temp_c_per_temp_c_filled_sum_15m"),
        F.avg("temp_c_filled").over(w_recent).alias("temp_c_filled_avg_5_prior"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("temp_c_per_temp_c_filled_count_15m_avg_6_prior", 0.95).alias("temp_c_per_temp_c_filled_count_15m_avg_6_prior_p95"),
        F.percentile_approx("temp_c_per_temp_c_filled_sum_15m", 0.95).alias("temp_c_per_temp_c_filled_sum_15m_p95"),
        F.percentile_approx("temp_c_per_temp_c_filled_sum_15m", [0.25, 0.5, 0.75]).alias("temp_c_per_temp_c_filled_sum_15m_quartiles"),
    )
    .orderBy(F.col("temp_c_per_temp_c_filled_count_15m_avg_6_prior_p95").desc())
)
