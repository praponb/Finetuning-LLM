from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy("prev_status_bits_plus_humidity", "id")
w4 = Window.orderBy(F.col("status_bits").desc(), "id")
w5 = Window.orderBy("status_bits_plus_humidity", "id").rowsBetween(-5, -1)
w6 = Window.orderBy("prev_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("reading_ts", "id")
w8 = Window.orderBy("prev_status_bits_plus_humidity_bucket", "id")
w9 = Window.orderBy("row_seq_2", "id")
w10 = Window.orderBy("prev_row_seq", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.orderBy("last_known_prev_temp_c", "id")
w12 = Window.orderBy("temp_c", "id")
w13 = Window.orderBy("next_temp_c")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_plus_humidity",
        F.lag("status_bits_plus_humidity").over(w_running).alias("prev_status_bits_plus_humidity"),
        F.lead("status_bits_plus_humidity").over(w_running).alias("next_status_bits_plus_humidity"),
        F.lead("temp_c").over(w_running).alias("next_temp_c"),
        F.lag("temp_c").over(w_running).alias("prev_temp_c"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("temp_c") < 37)
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 36.1, "High").when(F.col("temp_c") >= 21.4, "Medium").otherwise("Low"))
)

enriched = (
    filtered
    .withColumn("status_bits_avg_30m", F.avg("status_bits").over(w2))
    .withColumn("prev_status_bits_plus_humidity_bucket", F.ntile(4).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("pressure_avg_5_prior", F.avg("pressure").over(w5))
    .withColumn("last_known_prev_temp_c", F.last("prev_temp_c", ignorenulls=True).over(w6))
)

enriched_device_readings = (
    enriched
    .select(
        "id",
        "temp_c",
        "status_bits_plus_humidity",
        "prev_status_bits_plus_humidity",
        "next_temp_c",
        "row_seq",
        "temp_c_tier",
        "prev_status_bits_plus_humidity_bucket",
        "event_seq",
        "pressure_avg_5_prior",
        "last_known_prev_temp_c",
        F.lag("row_seq").over(w_part).alias("prev_row_seq"),
        F.lead("row_seq").over(w_part).alias("next_row_seq"),
        F.lag("status_bits_avg_30m").over(w_part).alias("prev_status_bits_avg_30m"),
        F.row_number().over(w_part).alias("row_seq_2"),
    )
)

with_windows = (
    enriched_device_readings
    .filter(F.col("row_seq_2") > 4)
    .select(
        "*",
        F.lead("next_row_seq", 1).over(w8).alias("next_next_row_seq"),
        F.lag("row_seq_2", 1).over(w9).alias("prev_row_seq_2"),
        F.avg("prev_status_bits_plus_humidity_bucket").over(w10).alias("prev_status_bits_plus_humidity_bucket_avg_6"),
    )
)

enriched_device_readings_2 = (
    with_windows
    .select(
        "id",
        "temp_c",
        "status_bits_plus_humidity",
        "prev_status_bits_plus_humidity",
        "row_seq",
        "temp_c_tier",
        "prev_status_bits_avg_30m",
        "next_next_row_seq",
        "prev_row_seq_2",
        "prev_status_bits_plus_humidity_bucket_avg_6",
        F.lag("prev_status_bits_plus_humidity_bucket_avg_6", 2).over(w11).alias("prev_prev_status_bits_plus_humidity_bucket_avg_6"),
        F.row_number().over(w12).alias("event_seq_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.rank().over(w13).alias("next_temp_c_rank"),
    )
)

result = (
    enriched_device_readings_2
    .groupBy(
        "seq_no",
        F.when(F.col("prev_prev_status_bits_plus_humidity_bucket_avg_6") >= 8, "LARGE").otherwise("SMALL").alias("prev_prev_status_bits_plus_humidity_bucket_avg_6_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.max("seq_no").alias("max_seq_no"),
        F.min("temp_c").alias("min_temp_c"),
        F.percentile_approx("prev_prev_status_bits_plus_humidity_bucket_avg_6", 0.5).alias("prev_prev_status_bits_plus_humidity_bucket_avg_6_p50"),
    )
    .orderBy(F.col("prev_prev_status_bits_plus_humidity_bucket_avg_6_p50").desc())
)
