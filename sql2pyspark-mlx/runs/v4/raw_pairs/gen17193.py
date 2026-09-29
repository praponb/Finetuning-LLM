from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w3 = Window.orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity_whole", "id")
w5 = Window.partitionBy("device_code").orderBy("status_bits_avg_6", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("running_status_bits").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy("greatest_temp_c_status_bits", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w9 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w11 = Window.partitionBy("device_code").orderBy(F.col("least_event_seq_humidity_whole_bucket").asc_nulls_last(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") < 8)
    .withColumn("humidity_whole", F.floor("humidity"))
    .withColumn("greatest_temp_c_status_bits", F.greatest("temp_c", "status_bits"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("status_bits").over(w).alias("running_status_bits"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("status_bits").over(w3).alias("status_bits_avg_6"),
        F.ntile(4).over(w4).alias("humidity_whole_bucket"),
    )
)

metrics = (
    with_windows
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("humidity_avg_6_prior", F.avg("humidity").over(w7))
)

filtered = (
    metrics
    .filter(F.col("temp_c").between(31, 35))
    .select(
        "*",
        F.least("event_seq", "humidity_whole_bucket").alias("least_event_seq_humidity_whole_bucket"),
    )
)

with_windows_device_readings = (
    filtered
    .withColumn("next_event_seq", F.lead("event_seq", 1).over(w8))
    .withColumn("status_bits_sum_30m", F.sum("status_bits").over(w9))
    .withColumn("humidity_percent_rank", F.percent_rank().over(w_grp))
)

deduped = (
    with_windows_device_readings
    .withColumn("row_num", F.row_number().over(w11))
    .filter(F.col("row_num") == 1)
)

daily = (
    deduped
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
)

result = (
    daily
    .select("hour_window", "dv_04_count")
    .orderBy(F.col("dv_04_count").desc())
)
