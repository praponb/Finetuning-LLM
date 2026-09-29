from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())
w6 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.orderBy(F.col("prev_status_bits").desc(), "id")
w10 = Window.orderBy("temp_c_per_humidity", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp")) & (F.col("temp_c") <= 37))
    .select(
        "*",
        F.round((F.col("temp_c") / F.col("humidity")), 2).alias("temp_c_per_humidity"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "temp_c_per_humidity",
        F.avg("humidity").over(w).alias("humidity_avg_5"),
        F.last("humidity", ignorenulls=True).over(w2).alias("last_known_humidity"),
        F.lag("status_bits", 1, 0).over(w3).alias("prev_status_bits"),
        F.sum("status_bits").over(w4).alias("running_status_bits"),
        F.rank().over(w5).alias("temp_c_rank"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "reading_ts",
        "status_bits",
        "temp_c_per_humidity",
        "last_known_humidity",
        "prev_status_bits",
        "running_status_bits",
        F.max("temp_c_rank").over(w6).alias("running_max_temp_c_rank"),
        F.sum("temp_c_rank").over(w6).alias("running_sum_temp_c_rank"),
        F.min("temp_c_rank").over(w6).alias("running_min_temp_c_rank"),
        F.avg("temp_c_rank").over(w6).alias("running_avg_temp_c_rank"),
        F.avg("last_known_humidity").over(w7).alias("last_known_humidity_avg_2h"),
    )
)

with_windows = (
    metrics_device_readings
    .select(
        "*",
        F.avg("status_bits").over(w_running).alias("status_bits_avg_1h"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .filter(F.col("running_max_temp_c_rank").between(3.2, 6.9))
    .select(
        "*",
        F.lag("running_min_temp_c_rank").over(w10).alias("prev_running_min_temp_c_rank"),
        F.lag("running_sum_temp_c_rank").over(w10).alias("prev_running_sum_temp_c_rank"),
        F.lead("running_sum_temp_c_rank").over(w10).alias("next_running_sum_temp_c_rank"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "reading_ts",
        "temp_c_per_humidity",
        "last_known_humidity",
        "prev_status_bits",
        "running_max_temp_c_rank",
        "running_sum_temp_c_rank",
        "running_min_temp_c_rank",
        "running_avg_temp_c_rank",
    )
    .orderBy("prev_status_bits")
)
