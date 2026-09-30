from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w_part = Window.orderBy("temp_c_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_avg_status_bits").desc(), "id")
w7 = Window.orderBy("running_count_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("running_count_status_bits").desc(), "id")
w9 = Window.orderBy("prev_humidity", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select("*", F.length("device_code").alias("device_code_length"))
)

with_windows = (
    filtered
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        F.avg("temp_c").over(w).alias("temp_c_avg_6"),
        F.lag("humidity", 1, 0).over(w2).alias("prev_humidity"),
        F.avg("status_bits").over(w3).alias("status_bits_avg_1h"),
        F.percent_rank().over(w4).alias("temp_c_percent_rank"),
    )
)

with_windows_device_readings = (
    with_windows
    .select(
        "id",
        "humidity",
        "prev_humidity",
        F.avg("status_bits").over(w_part).alias("running_avg_status_bits"),
        F.count("status_bits").over(w_part).alias("running_count_status_bits"),
        F.sum("status_bits").over(w_part).alias("running_sum_status_bits"),
    )
)

scored = (
    with_windows_device_readings
    .select(
        "id",
        "humidity",
        "prev_humidity",
        F.row_number().over(w6).alias("row_seq"),
        F.first("running_avg_status_bits", ignorenulls=True).over(w7).alias("first_known_running_avg_status_bits"),
        F.row_number().over(w8).alias("event_seq"),
        F.avg("running_sum_status_bits").over(w9).alias("running_sum_status_bits_avg_3"),
    )
)

cleaned = (
    scored
    .filter(F.col("row_seq") < 7)
    .select(
        "id",
        "humidity",
        "prev_humidity",
        "first_known_running_avg_status_bits",
        "event_seq",
        F.round((F.col("row_seq") / F.col("event_seq")), 2).alias("row_seq_per_event_seq"),
    )
)

report = (
    cleaned
    .select(
        "id",
        "humidity",
        "prev_humidity",
        "first_known_running_avg_status_bits",
        "row_seq_per_event_seq",
    )
    .orderBy(F.col("row_seq_per_event_seq").desc(), F.col("prev_humidity").desc())
)

result = (
    report
    .select(
        "id",
        "prev_humidity",
        "first_known_running_avg_status_bits",
        "row_seq_per_event_seq",
    )
    .orderBy(F.col("row_seq_per_event_seq").desc_nulls_first(), "prev_humidity")
)
