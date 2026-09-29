from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts_hour").desc_nulls_first(), "id")
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("humidity", "id")
w4 = Window.orderBy(F.col("running_count_firmware_length").asc_nulls_last())
w5 = Window.orderBy("running_sum_firmware_length", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("pressure") < 1019)
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        F.hour("reading_ts").alias("reading_ts_hour"),
        F.lower("firmware").alias("firmware_lower"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "humidity", "firmware")
)

filtered_device_readings = (
    deduped
    .filter(F.col("firmware") != "1.1.0")
    .select("*", F.length("firmware").alias("firmware_length"))
)

with_windows = (
    filtered_device_readings
    .select(
        "id",
        "firmware",
        "firmware_length",
        F.min("firmware_length").over(w2).alias("running_min_firmware_length"),
        F.sum("firmware_length").over(w2).alias("running_sum_firmware_length"),
        F.count("firmware_length").over(w2).alias("running_count_firmware_length"),
        F.lead("firmware_length", 1, 0).over(w3).alias("next_firmware_length"),
    )
)

with_windows_device_readings = (
    with_windows
    .filter(F.col("running_sum_firmware_length") > 250)
    .withColumn("running_count_firmware_length_rank", F.rank().over(w4))
    .withColumn("running_min_firmware_length_avg_2", F.avg("running_min_firmware_length").over(w5))
)

valid = (
    with_windows_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "firmware",
        "firmware_length",
        "running_min_firmware_length",
        "running_sum_firmware_length",
        "running_count_firmware_length",
        "next_firmware_length",
        "running_min_firmware_length_avg_2",
        (F.col("running_min_firmware_length") - F.col("running_count_firmware_length")).alias("running_min_firmware_length_minus_running_count_firmware_length"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("running_sum_firmware_length") <= 140)
    .select(
        "*",
        (F.col("running_min_firmware_length") - F.col("running_min_firmware_length_minus_running_count_firmware_length")).alias("running_min_firmware_length_minus_running_min_firmware_length_minus_running_count_firmware_length"),
    )
)

result = (
    valid_device_readings
    .groupBy(
        F.when(F.col("running_min_firmware_length") >= 72, "HIGH").otherwise("LOW").alias("running_min_firmware_length_tier"),
    )
    .agg(
        F.percentile_approx("running_min_firmware_length_minus_running_count_firmware_length", [0.25, 0.5, 0.75]).alias("running_min_firmware_length_minus_running_count_firmware_length_quartiles"),
        F.percentile_approx("running_sum_firmware_length", 0.5).alias("running_sum_firmware_length_p50"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy(F.col("running_sum_firmware_length_p50").asc_nulls_last())
)
