from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("cumulative_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("cumulative_temp_c", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-03-01")
    .withColumn("status_bits_rounded", F.round("status_bits", 2))
)

metrics = (
    valid
    .select(
        "id",
        "device_code",
        "status_bits",
        "firmware",
        F.avg("status_bits_rounded").over(w).alias("status_bits_rounded_avg_30m"),
        F.sum("temp_c").over(w2).alias("cumulative_temp_c"),
        F.sum("humidity").over(w3).alias("running_humidity"),
    )
)

cleaned = (
    metrics
    .filter(F.col("status_bits_rounded_avg_30m").between(44, 61))
    .select(
        "*",
        F.greatest("status_bits", "cumulative_temp_c").alias("greatest_status_bits_cumulative_temp_c"),
    )
)

metrics_device_readings = (
    cleaned
    .select(
        "id",
        "status_bits",
        "firmware",
        "status_bits_rounded_avg_30m",
        F.first("cumulative_temp_c", ignorenulls=True).over(w4).alias("first_known_cumulative_temp_c"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    metrics_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 13, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.percentile_approx("status_bits", 0.9).alias("status_bits_p90"),
        F.round(F.avg("first_known_cumulative_temp_c"), 2).alias("avg_first_known_cumulative_temp_c"),
    )
    .filter(F.col("event_count") < 1000)
)
