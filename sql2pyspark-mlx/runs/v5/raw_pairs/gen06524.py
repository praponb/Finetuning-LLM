from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure_share", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy(F.col("device_code_count_status_bits").desc_nulls_last())
w_part = Window.orderBy("temp_c", "id")
w8 = Window.orderBy("status_bits", "id").rowsBetween(-2, -1)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 08:00:00"))) & (F.to_date("reading_ts") < "2025-06-04"))
    .select("*", F.upper("firmware").alias("firmware_upper"))
)

metrics = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("device_code_count_status_bits", F.count("status_bits").over(w2))
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_upper",
        "device_code_count_status_bits",
        "pressure_share",
        F.sum("device_code_count_status_bits").over(w4).alias("device_code_count_status_bits_sum_1h"),
        F.avg("device_code_count_status_bits").over(w5).alias("device_code_count_status_bits_avg_5"),
        F.percent_rank().over(w_ordered).alias("device_code_count_status_bits_percent_rank"),
    )
)

metrics_device_readings = (
    enriched
    .filter(F.col("pressure_share") >= 0.44)
    .select(
        "*",
        F.lag("device_code_count_status_bits", 2).over(w_part).alias("prev_device_code_count_status_bits"),
        F.avg("pressure_share").over(w8).alias("pressure_share_avg_2_prior"),
    )
)

report = (
    metrics_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware_upper",
        "device_code_count_status_bits",
        "pressure_share",
        "device_code_count_status_bits_sum_1h",
    )
    .orderBy(F.col("temp_c").asc_nulls_last())
)

result = (
    report
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "device_code_count_status_bits",
        "pressure_share",
    )
)
