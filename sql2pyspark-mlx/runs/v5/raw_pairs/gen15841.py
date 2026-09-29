from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-6, -1)
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts_period").desc(), "id")
w6 = Window.orderBy("temp_c")
w7 = Window.partitionBy("device_code").orderBy("temp_c_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("cumulative_pressure_sum_30m", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("device_code")
w10 = Window.partitionBy("device_code").orderBy("row_seq", "id")
w_hist = Window.partitionBy("device_code").orderBy("temp_c_cume_dist", "id")
w12 = Window.partitionBy("device_code").orderBy("last7_count_next_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("running_min_prev_pressure_sum_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp")) & (F.col("pressure") > 1013))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.date_trunc("YEAR", "reading_ts").alias("reading_ts_period"),
        (F.col("temp_c") * F.col("humidity")).alias("temp_c_times_humidity"),
    )
)

with_windows = (
    base
    .filter(F.col("temp_c_times_humidity").between(36, 66))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "reading_ts_period",
        F.lead("pressure", 2, 0).over(w).alias("next_pressure"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_6_prior"),
        F.sum("pressure").over(w3).alias("pressure_sum_30m"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "reading_ts_period",
        "next_pressure",
        "pressure_sum_30m",
        F.sum("pressure_sum_30m").over(w4).alias("cumulative_pressure_sum_30m"),
        F.row_number().over(w5).alias("row_seq"),
        F.cume_dist().over(w6).alias("temp_c_cume_dist"),
    )
)

scored = (
    enriched
    .withColumn("last7_max_next_pressure", F.max("next_pressure").over(w7))
    .withColumn("last7_avg_next_pressure", F.avg("next_pressure").over(w7))
    .withColumn("last7_count_next_pressure", F.count("next_pressure").over(w7))
    .withColumn("last7_sum_next_pressure", F.sum("next_pressure").over(w7))
)

scored_device_readings = (
    scored
    .select(
        "id",
        "device_code",
        "temp_c",
        "reading_ts_period",
        "pressure_sum_30m",
        "last7_count_next_pressure",
        F.avg("temp_c_cume_dist").over(w_recent).alias("temp_c_cume_dist_avg_2_prior"),
        F.corr("temp_c", "pressure").over(w9).alias("corr_temp_c_pressure"),
        F.row_number().over(w10).alias("event_seq"),
        F.lag("pressure_sum_30m", 2).over(w_hist).alias("prev_pressure_sum_30m"),
    )
)

scored_device_readings_2 = (
    scored_device_readings
    .select(
        "id",
        "pressure_sum_30m",
        "last7_count_next_pressure",
        "temp_c_cume_dist_avg_2_prior",
        F.min("prev_pressure_sum_30m").over(w12).alias("running_min_prev_pressure_sum_30m"),
        F.max("prev_pressure_sum_30m").over(w12).alias("running_max_prev_pressure_sum_30m"),
        F.avg("prev_pressure_sum_30m").over(w12).alias("running_avg_prev_pressure_sum_30m"),
    )
)

with_windows_device_readings = (
    scored_device_readings_2
    .select(
        "*",
        F.max("last7_count_next_pressure").over(w_running).alias("running_max_last7_count_next_pressure"),
        F.avg("last7_count_next_pressure").over(w_running).alias("running_avg_last7_count_next_pressure"),
        F.min("last7_count_next_pressure").over(w_running).alias("running_min_last7_count_next_pressure"),
    )
)

result = (
    with_windows_device_readings
    .groupBy(
        F.when(F.col("running_max_last7_count_next_pressure") >= 14.8, "LARGE").otherwise("SMALL").alias("running_max_last7_count_next_pressure_tier"),
    )
    .agg(
        F.percentile_approx("running_min_last7_count_next_pressure", 0.5).alias("running_min_last7_count_next_pressure_p50"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_count_next_pressure", 0.9).alias("last7_count_next_pressure_p90"),
        F.sum("running_min_last7_count_next_pressure").alias("total_running_min_last7_count_next_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_avg_prev_pressure_sum_30m", [0.25, 0.5, 0.75]).alias("running_avg_prev_pressure_sum_30m_quartiles"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
