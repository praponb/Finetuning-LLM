from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w6 = Window.orderBy(F.col("device_code_stddev_samp_temp_c").desc_nulls_first())
w7 = Window.partitionBy("device_code").orderBy("last_known_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("is_new_session", "id")
w9 = Window.partitionBy("device_code").orderBy(F.col("is_new_session").desc(), "id")
w10 = Window.orderBy(F.col("status_bits").desc())
w_part = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

metrics = (
    sessions_device_readings
    .withColumn("device_code_stddev_samp_temp_c", F.stddev_samp("temp_c").over(w3))
    .withColumn("first_known_humidity", F.first("humidity", ignorenulls=True).over(w4))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w5))
)

with_windows = (
    metrics
    .select(
        "*",
        F.rank().over(w6).alias("device_code_stddev_samp_temp_c_rank"),
        F.last("device_code_stddev_samp_temp_c", ignorenulls=True).over(w7).alias("last_known_device_code_stddev_samp_temp_c"),
    )
)

metrics_device_readings = (
    with_windows
    .select(
        "*",
        F.round((F.col("device_code_stddev_samp_temp_c") / F.sum("device_code_stddev_samp_temp_c").over(w3)), 4).alias("device_code_stddev_samp_temp_c_share"),
        F.lag("last_known_device_code_stddev_samp_temp_c", 1).over(w8).alias("prev_last_known_device_code_stddev_samp_temp_c"),
        F.ntile(3).over(w5).alias("temp_c_bucket"),
        F.row_number().over(w9).alias("row_seq"),
        F.dense_rank().over(w10).alias("status_bits_dense_rank"),
    )
)

with_windows_device_readings = (
    metrics_device_readings
    .select(
        "*",
        F.sum("pressure").over(w_part).alias("running_pressure"),
        F.round((F.col("status_bits_dense_rank") / F.sum("status_bits_dense_rank").over(w3)), 4).alias("status_bits_dense_rank_share"),
    )
)

cleaned = (
    with_windows_device_readings
    .filter(F.col("running_pressure") >= 3970)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "is_new_session",
        "island_id",
        "device_code_stddev_samp_temp_c",
        "last_known_humidity",
        "device_code_stddev_samp_temp_c_rank",
        "device_code_stddev_samp_temp_c_share",
        "temp_c_bucket",
        "status_bits_dense_rank",
        "running_pressure",
        "status_bits_dense_rank_share",
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "is_new_session",
        "device_code_stddev_samp_temp_c",
        "last_known_humidity",
        "device_code_stddev_samp_temp_c_rank",
    )
)
