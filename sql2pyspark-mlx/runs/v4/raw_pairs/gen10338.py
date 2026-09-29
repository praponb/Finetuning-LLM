from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id")
w2 = Window.orderBy(F.col("humidity").desc(), "id")
w3 = Window.orderBy("temp_c", "id")
w4 = Window.orderBy(F.col("status_bits").desc())
w5 = Window.orderBy(F.col("pressure").asc_nulls_first())
w6 = Window.orderBy(F.col("temp_c").desc(), "id")
w7 = Window.orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("firmware").isin(["1.1.0", "1.0.3"]), "Tier 1").otherwise("Other").alias("firmware_group"),
    )
)

valid = (
    cleaned
    .filter((F.col("reading_ts") > F.lit("2025-09-01 15:00:00").cast("timestamp")) & (F.col("firmware") == "2.0.1"))
    .select(
        "*",
        F.when(F.col("pressure") >= 1032, "High").when(F.col("pressure") >= 989, "Medium").otherwise("Low").alias("pressure_band"),
        F.date_trunc("year", "reading_ts").alias("reading_ts_period"),
    )
)

with_windows = (
    valid
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("temp_c_bucket", F.ntile(10).over(w3))
    .withColumn("status_bits_dense_rank", F.dense_rank().over(w4))
    .withColumn("pressure_dense_rank", F.dense_rank().over(w5))
)

with_windows_device_readings = (
    with_windows
    .select(
        "*",
        F.ntile(5).over(w6).alias("temp_c_bucket_2"),
        F.ntile(5).over(w7).alias("humidity_bucket"),
    )
)

result = (
    with_windows_device_readings
    .groupBy(
        F.date_trunc("YEAR", "reading_ts_period").alias("year_start"),
        F.date_trunc("QUARTER", "reading_ts_period").alias("quarter_start"),
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.min("humidity").alias("min_humidity"),
        F.percentile_approx("temp_c_bucket_2", [0.25, 0.5, 0.75]).alias("temp_c_bucket_2_quartiles"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
)
