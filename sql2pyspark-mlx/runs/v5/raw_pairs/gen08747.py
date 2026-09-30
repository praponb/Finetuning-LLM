from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("running_max_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 08:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.col("temp_c") >= 34)
    .select(
        "*",
        F.max("humidity").over(w2).alias("running_max_humidity"),
        F.min("humidity").over(w2).alias("running_min_humidity"),
        F.sum("humidity").over(w2).alias("running_sum_humidity"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w3)), 4).alias("temp_c_share"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "dedup_rank",
        "running_sum_humidity",
        "temp_c_share",
        F.sum("humidity_filled").over(w4).alias("running_humidity_filled"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "running_sum_humidity",
        "row_seq",
    )
    .orderBy(F.col("status_bits").asc_nulls_first(), "device_code")
)
