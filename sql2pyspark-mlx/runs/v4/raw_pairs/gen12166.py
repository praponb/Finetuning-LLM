from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")
w6 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("corr_humidity_pressure").desc())
w8 = Window.partitionBy("device_code").orderBy("first_known_cumulative_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("status_bits")
w10 = Window.partitionBy("device_code").orderBy(F.col("running_humidity").desc())
w11 = Window.partitionBy("device_code").orderBy("rn", "id")

with_windows = (
    df_device_readings
    .withColumn("status_bits_bucket", F.ntile(5).over(w))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w2))
    .withColumn("running_pressure", F.sum("pressure").over(w3))
    .withColumn("running_humidity", F.sum("humidity").over(w4))
)

with_windows_device_readings = (
    with_windows
    .select(
        "*",
        F.corr("humidity", "pressure").over(w5).alias("corr_humidity_pressure"),
        F.sum("status_bits").over(w5).alias("device_code_sum_status_bits"),
        F.first("cumulative_status_bits", ignorenulls=True).over(w6).alias("first_known_cumulative_status_bits"),
    )
)

scored = (
    with_windows_device_readings
    .withColumn("corr_humidity_pressure_dense_rank", F.dense_rank().over(w7))
    .withColumn("cumulative_first_known_cumulative_status_bits", F.sum("first_known_cumulative_status_bits").over(w8))
    .withColumn("status_bits_dense_rank", F.dense_rank().over(w9))
)

valid = (
    scored
    .filter(F.col("status_bits_bucket") < 3)
    .select(
        "*",
        F.floor("first_known_cumulative_status_bits").alias("first_known_cumulative_status_bits_whole"),
    )
)

latest = (
    valid
    .withColumn("rn", F.rank().over(w10))
    .filter(F.col("rn") == 1)
)

latest_device_readings = (
    latest
    .withColumn("latest_rank", F.row_number().over(w11))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_device_readings
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.max("reading_ts").alias("last_seen"),
        F.sum("cumulative_first_known_cumulative_status_bits").alias("total_cumulative_first_known_cumulative_status_bits"),
        F.percentile_approx("device_code_sum_status_bits", [0.25, 0.5, 0.75]).alias("device_code_sum_status_bits_quartiles"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
