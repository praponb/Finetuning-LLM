from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure_minus_status_bits", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("humidity").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("status_bits").between(9, 11))
    .withColumn("firmware_group", F.when(F.col("firmware").isin(["1.0.3", "2.0.1"]), "Priority").otherwise("Other"))
)

valid = (
    cleaned
    .filter(F.col("humidity").between(44, 47))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_group",
        (F.col("pressure") - F.col("status_bits")).alias("pressure_minus_status_bits"),
    )
)

cleaned_device_readings = (
    valid
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware_group",
        "pressure_minus_status_bits",
        F.ceil("pressure").alias("pressure_whole"),
    )
)

metrics = (
    cleaned_device_readings
    .filter(F.col("pressure_whole") < 22)
    .select(
        "*",
        F.min("pressure").over(w).alias("last4_min_pressure"),
        F.sum("pressure").over(w).alias("last4_sum_pressure"),
        F.avg("pressure").over(w).alias("last4_avg_pressure"),
        F.ntile(10).over(w2).alias("humidity_bucket"),
    )
)

result = (
    metrics
    .withColumn("pressure_whole_tier", F.when(F.col("pressure_whole") >= 70, "LARGE").otherwise("SMALL"))
    .withColumn("last4_avg_pressure_tier", F.when(F.col("last4_avg_pressure") >= 1023, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["pressure_whole_tier", "last4_avg_pressure_tier"], ["pressure_whole_tier"], []],
        "pressure_whole_tier",
        "last4_avg_pressure_tier",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("last4_avg_pressure", 0.95).alias("last4_avg_pressure_p95"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.round(F.avg("humidity_bucket"), 2).alias("avg_humidity_bucket"),
        F.percentile_approx("pressure_minus_status_bits", 0.5).alias("pressure_minus_status_bits_p50"),
        F.bit_or("status_bits").alias("status_bits_bit_or_2"),
        F.grouping_id().alias("grouping_id"),
    )
)
