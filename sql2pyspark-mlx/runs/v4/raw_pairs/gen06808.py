from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("firmware_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("humidity_whole", "id")
w5 = Window.orderBy("humidity_whole", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("cumulative_firmware_length")

latest = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "row_num",
        F.concat_ws("-", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

valid = (
    base
    .filter(F.col("row_num") != 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "row_num",
        F.floor("humidity").alias("humidity_whole"),
    )
)

filtered = (
    valid
    .filter(F.col("status_bits") >= 4)
    .select(
        "id",
        "humidity",
        "firmware",
        "humidity_whole",
        F.length("firmware").alias("firmware_length"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("humidity").over(w2).alias("cumulative_humidity"),
        F.sum("firmware_length").over(w3).alias("cumulative_firmware_length"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("firmware_length").over(w5).alias("firmware_length_avg_2_prior"),
    )
)

result = (
    metrics
    .withColumn("cumulative_cumulative_humidity", F.sum("cumulative_humidity").over(w2))
    .withColumn("cumulative_firmware_length_cume_dist", F.cume_dist().over(w6))
)
