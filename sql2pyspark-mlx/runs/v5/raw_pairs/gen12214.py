from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("device_code_stddev_samp_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("status_bits", "id").rowsBetween(-3, -1)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 06:00:00"))) & (F.col("firmware") == "2.0.1"))
    .withColumn("firmware_length", F.length("firmware"))
    .withColumn("firmware_lower", F.lower("firmware"))
)

metrics = (
    cleaned
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.ntile(4).over(w).alias("status_bits_bucket"),
        F.dense_rank().over(w2).alias("humidity_dense_rank"),
        F.stddev_samp("status_bits").over(w3).alias("device_code_stddev_samp_status_bits"),
    )
)

metrics_device_readings = (
    metrics
    .withColumn("last_known_humidity_dense_rank", F.last("humidity_dense_rank", ignorenulls=True).over(w4))
    .withColumn("humidity_dense_rank_avg_3_prior", F.avg("humidity_dense_rank").over(w5))
)

filtered = (
    metrics_device_readings
    .filter(F.col("device_code_stddev_samp_status_bits") == 7)
    .select(
        "id",
        "status_bits",
        "firmware",
        "device_code_stddev_samp_status_bits",
        "humidity_dense_rank_avg_3_prior",
        F.round((F.col("status_bits") * 1.15), 2).alias("status_bits_scaled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "status_bits",
        "firmware",
        "humidity_dense_rank_avg_3_prior",
        "status_bits_scaled",
    )
    .orderBy(F.col("status_bits").asc_nulls_last())
)
