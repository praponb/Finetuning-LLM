from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy(F.col("status_bits").desc(), "id")
w4 = Window.orderBy("device_code_count_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("status_bits", "id").rowsBetween(-2, -1)
w6 = Window.orderBy(F.col("pressure").desc())
w7 = Window.orderBy("event_seq", "id")
w8 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("status_bits").between(5, 9))
    .withColumn("temp_c_scaled", F.round((F.col("temp_c") * 1.07), 2))
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "dedup_rank",
        F.count("status_bits").over(w2).alias("device_code_count_status_bits"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "status_bits",
        "dedup_rank",
        "event_seq",
        F.last("device_code_count_status_bits", ignorenulls=True).over(w4).alias("last_known_device_code_count_status_bits"),
        F.avg("status_bits").over(w5).alias("status_bits_avg_2_prior"),
        F.cume_dist().over(w6).alias("pressure_cume_dist"),
        F.ntile(10).over(w7).alias("event_seq_bucket"),
        F.sum("status_bits").over(w8).alias("cumulative_status_bits"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("event_seq_bucket") >= 8, "HIGH").otherwise("LOW").alias("event_seq_bucket_tier"),
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.avg("event_seq").alias("avg_event_seq"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
    .filter(F.col("avg_event_seq") > 0)
    .orderBy(F.col("avg_event_seq").desc())
)
