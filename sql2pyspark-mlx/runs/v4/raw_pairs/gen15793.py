from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("temp_c").desc(), "id")
w3 = Window.orderBy(F.col("temp_c").desc())
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w8 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w9 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy("running_humidity", "id")
w11 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("humidity") > 60)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.substring("firmware", 1, 2).alias("firmware_prefix"),
    )
)

metrics = (
    valid
    .withColumn("running_humidity", F.sum("humidity").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("temp_c_dense_rank", F.dense_rank().over(w3))
    .withColumn("temp_c_bucket", F.ntile(10).over(w4))
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.sum("temp_c_bucket").over(w5).alias("temp_c_bucket_sum_1h"),
        F.avg("humidity").over(w6).alias("humidity_avg_2_prior"),
        F.ntile(3).over(w7).alias("temp_c_bucket_2"),
        F.rank().over(w8).alias("humidity_rank"),
        F.sum("temp_c").over(w9).alias("cumulative_temp_c"),
    )
)

scored = (
    metrics_device_readings
    .withColumn("next_row_seq", F.lead("row_seq", 1).over(w10))
    .withColumn("status_bits_max_15m", F.max("status_bits").over(w11))
)

result = (
    scored
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity")).otherwise(0)).alias("dv_04_humidity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("temp_c_bucket_2")).otherwise(0)).alias("dv_01_temp_c_bucket_2"),
        F.min("reading_ts").alias("started_at"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
    )
    .orderBy(F.col("dv_04_rows").desc())
)
