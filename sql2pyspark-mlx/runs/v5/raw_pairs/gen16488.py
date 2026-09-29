from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w5 = Window.orderBy("prev_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_status_bits", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("prev_status_bits_rounded", "id")
w_hist = Window.orderBy(F.col("temp_c").desc(), "id")
w9 = Window.orderBy("prev_status_bits_rounded_minus_prev_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("corr_humidity_temp_c", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.orderBy(F.col("prev_status_bits_rounded_minus_prev_status_bits").desc(), "id")

with_windows = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "firmware",
        F.count("humidity").over(w).alias("humidity_count_30m"),
        F.lag("status_bits", 1).over(w2).alias("prev_status_bits"),
        F.corr("humidity", "temp_c").over(w3).alias("corr_humidity_temp_c"),
        F.ntile(4).over(w4).alias("status_bits_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .withColumn("prev_status_bits_whole", F.floor("prev_status_bits"))
)

cleaned = (
    filtered
    .filter(F.hour("reading_ts").between(6, 13))
    .select(
        "id",
        "temp_c",
        "firmware",
        "humidity_count_30m",
        "prev_status_bits",
        "corr_humidity_temp_c",
        F.round("prev_status_bits", 2).alias("prev_status_bits_rounded"),
    )
)

with_windows_device_readings = (
    cleaned
    .filter(F.col("corr_humidity_temp_c").between(-0.5, -0.1))
    .withColumn("running_max_temp_c", F.max("temp_c").over(w5))
    .withColumn("running_sum_temp_c", F.sum("temp_c").over(w5))
    .withColumn("running_count_temp_c", F.count("temp_c").over(w5))
    .withColumn("corr_humidity_temp_c_avg_5_prior", F.avg("corr_humidity_temp_c").over(w6))
)

cleaned_device_readings = (
    with_windows_device_readings
    .filter(F.col("humidity_count_30m") != 8)
    .select(
        "*",
        (F.col("prev_status_bits_rounded") - F.col("prev_status_bits")).alias("prev_status_bits_rounded_minus_prev_status_bits"),
    )
)

with_windows_device_readings_2 = (
    cleaned_device_readings
    .withColumn("prev_prev_status_bits_rounded_minus_prev_status_bits", F.lag("prev_status_bits_rounded_minus_prev_status_bits", 1, 0).over(w7))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("cumulative_running_sum_temp_c", F.sum("running_sum_temp_c").over(w9))
    .withColumn("running_sum_temp_c_avg_2", F.avg("running_sum_temp_c").over(w10))
    .withColumn("event_seq", F.row_number().over(w11))
)

result = (
    with_windows_device_readings_2
    .rollup(
        F.when(F.col("running_sum_temp_c_avg_2") >= 173, "PREMIUM").otherwise("STANDARD").alias("running_sum_temp_c_avg_2_tier"),
        F.when(F.col("running_sum_temp_c") >= 175, "LARGE").otherwise("SMALL").alias("running_sum_temp_c_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.min("event_seq").alias("min_event_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_prev_status_bits_rounded_minus_prev_status_bits", 0.5).alias("prev_prev_status_bits_rounded_minus_prev_status_bits_p50"),
    )
    .orderBy(F.col("min_event_seq").desc_nulls_first())
)
