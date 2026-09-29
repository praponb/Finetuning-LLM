from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("cumulative_status_bits", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("cumulative_status_bits", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("row_seq").desc())
w7 = Window.partitionBy("device_code").orderBy("last3_count_first_known_temp_c", "id")
w8 = Window.partitionBy("device_code").orderBy("last3_min_first_known_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("last3_min_first_known_temp_c", "id")
w10 = Window.partitionBy("device_code").orderBy("prev_cumulative_status_bits", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("cumulative_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("event_seq", "id")

filtered = (
    df_device_readings
    .filter(F.col("temp_c") > 25)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "device_code", "reading_ts", "temp_c", "status_bits")
)

enriched = (
    latest
    .select(
        "id",
        "device_code",
        F.first("temp_c", ignorenulls=True).over(w2).alias("first_known_temp_c"),
        F.sum("status_bits").over(w3).alias("cumulative_status_bits"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "device_code",
        "first_known_temp_c",
        "cumulative_status_bits",
        F.min("first_known_temp_c").over(w4).alias("last3_min_first_known_temp_c"),
        F.avg("first_known_temp_c").over(w4).alias("last3_avg_first_known_temp_c"),
        F.count("first_known_temp_c").over(w4).alias("last3_count_first_known_temp_c"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_device_readings = (
    deduped
    .filter(F.col("last3_min_first_known_temp_c") >= 30)
    .select(
        "id",
        "device_code",
        "cumulative_status_bits",
        "last3_min_first_known_temp_c",
        "last3_count_first_known_temp_c",
        "row_seq",
        F.lead("cumulative_status_bits").over(w7).alias("next_cumulative_status_bits"),
        F.lag("cumulative_status_bits").over(w7).alias("prev_cumulative_status_bits"),
        F.lead("last3_avg_first_known_temp_c").over(w7).alias("next_last3_avg_first_known_temp_c"),
        F.lag("last3_avg_first_known_temp_c").over(w7).alias("prev_last3_avg_first_known_temp_c"),
        F.row_number().over(w7).alias("event_seq"),
        F.last("last3_avg_first_known_temp_c", ignorenulls=True).over(w8).alias("last_known_last3_avg_first_known_temp_c"),
    )
)

metrics = (
    enriched_device_readings
    .select(
        "id",
        "device_code",
        "last3_min_first_known_temp_c",
        "row_seq",
        "next_cumulative_status_bits",
        "next_last3_avg_first_known_temp_c",
        "prev_last3_avg_first_known_temp_c",
        "event_seq",
        F.row_number().over(w9).alias("event_seq_2"),
        F.avg("cumulative_status_bits").over(w10).alias("cumulative_status_bits_avg_2"),
        F.sum("next_last3_avg_first_known_temp_c").over(w11).alias("cumulative_next_last3_avg_first_known_temp_c"),
        F.lag("prev_last3_avg_first_known_temp_c", 1).over(w_running).alias("prev_prev_last3_avg_first_known_temp_c"),
    )
)

result = (
    metrics
    .select(
        "id",
        "prev_last3_avg_first_known_temp_c",
        "event_seq",
        "cumulative_status_bits_avg_2",
        "prev_prev_last3_avg_first_known_temp_c",
    )
)
