from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("firmware_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code")
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.orderBy(F.col("device_code_count_prev_firmware_length").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.length("firmware").alias("firmware_length"),
        F.when(F.col("pressure") >= 1025, "High").when(F.col("pressure") >= 1003, "Medium").otherwise("Low").alias("pressure_level"),
    )
)

metrics = (
    valid
    .withColumn("firmware_length_avg_3", F.avg("firmware_length").over(w))
    .withColumn("cumulative_pressure", F.sum("pressure").over(w2))
    .withColumn("prev_firmware_length", F.lag("firmware_length", 2, 0).over(w3))
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware_length",
        "pressure_level",
        "cumulative_pressure",
        F.lag("cumulative_pressure").over(w4).alias("prev_cumulative_pressure"),
        F.lag("pressure").over(w4).alias("prev_pressure"),
        F.lead("pressure").over(w4).alias("next_pressure"),
        F.row_number().over(w4).alias("event_seq"),
        F.count("prev_firmware_length").over(w5).alias("device_code_count_prev_firmware_length"),
    )
)

enriched = (
    metrics_device_readings
    .select(
        "*",
        F.avg("prev_pressure").over(w6).alias("prev_pressure_avg_2h"),
        F.ntile(3).over(w7).alias("device_code_count_prev_firmware_length_bucket"),
    )
)

result = (
    enriched
    .withColumn("prev_pressure_avg_2h_tier", F.when(F.col("prev_pressure_avg_2h") >= 1032, "PREMIUM").otherwise("STANDARD"))
    .withColumn("device_code_count_prev_firmware_length_bucket_tier", F.when(F.col("device_code_count_prev_firmware_length_bucket") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_pressure_avg_2h_tier", "device_code_count_prev_firmware_length_bucket_tier"], ["prev_pressure_avg_2h_tier"], []],
        "prev_pressure_avg_2h_tier",
        "device_code_count_prev_firmware_length_bucket_tier",
    )
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.percentile_approx("cumulative_pressure", [0.25, 0.5, 0.75]).alias("cumulative_pressure_quartiles"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.min("reading_ts").alias("started_at"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
)
