from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy(F.col("humidity_minus_first_known_humidity").desc(), "id")
w5 = Window.orderBy("humidity", "id").rowsBetween(-2, -1)
w_running = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.orderBy("first_known_humidity", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & F.col("status_bits").between(4, 11))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.round("temp_c", 0).alias("temp_c_rounded"),
    )
)

enriched = (
    filtered
    .withColumn("first_known_humidity", F.first("humidity", ignorenulls=True).over(w))
    .withColumn("next_humidity", F.lead("humidity", 2).over(w2))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4))
)

filtered_device_readings = (
    enriched
    .filter(F.col("first_known_humidity").isNotNull())
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "temp_c_rounded",
        "first_known_humidity",
        "next_humidity",
        "humidity_share",
        F.concat_ws("-", "device_code", "firmware").alias("device_code_firmware_key"),
    )
)

valid = (
    filtered_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & ~F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "temp_c_rounded",
        "first_known_humidity",
        "next_humidity",
        "humidity_share",
        "device_code_firmware_key",
        (F.col("humidity") - F.col("first_known_humidity")).alias("humidity_minus_first_known_humidity"),
    )
)

scored = (
    valid
    .filter(F.col("humidity") <= 55)
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.avg("first_known_humidity").over(w5).alias("first_known_humidity_avg_2_prior"),
        F.sum("humidity_minus_first_known_humidity").over(w_running).alias("running_humidity_minus_first_known_humidity"),
        F.sum("humidity_share").over(w7).alias("humidity_share_sum_30m"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "temp_c_rounded",
        "next_humidity",
        "humidity_share",
        "device_code_firmware_key",
        "first_known_humidity_avg_2_prior",
        "running_humidity_minus_first_known_humidity",
        F.lead("running_humidity_minus_first_known_humidity").over(w8).alias("next_running_humidity_minus_first_known_humidity"),
        F.lead("first_known_humidity_avg_2_prior").over(w8).alias("next_first_known_humidity_avg_2_prior"),
        F.lag("first_known_humidity_avg_2_prior").over(w8).alias("prev_first_known_humidity_avg_2_prior"),
        F.row_number().over(w8).alias("seq_no_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        "temp_c_rounded",
        "first_known_humidity_avg_2_prior",
        "next_first_known_humidity_avg_2_prior",
        "prev_first_known_humidity_avg_2_prior",
    )
    .orderBy(
        F.col("temp_c_rounded").asc_nulls_first(),
        "next_first_known_humidity_avg_2_prior",
    )
)
