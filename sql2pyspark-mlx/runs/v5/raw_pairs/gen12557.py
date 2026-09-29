from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("pressure", "id")
w8 = Window.partitionBy("device_code").orderBy("humidity_avg_1h")
w9 = Window.orderBy("seq_no")
w10 = Window.orderBy("corr_temp_c_humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .withColumn("temp_c_plus_status_bits", (F.col("temp_c") + F.col("status_bits")))
)

scored = (
    cleaned
    .filter(F.col("temp_c_plus_status_bits") >= 37)
    .withColumn("status_bits_count_1h", F.count("status_bits").over(w))
    .withColumn("corr_temp_c_humidity", F.corr("temp_c", "humidity").over(w2))
    .withColumn("running_temp_c_plus_status_bits", F.sum("temp_c_plus_status_bits").over(w3))
    .withColumn("humidity_avg_1h", F.avg("humidity").over(w))
)

metrics = (
    scored
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "corr_temp_c_humidity",
        F.sum("running_temp_c_plus_status_bits").over(w4).alias("running_running_temp_c_plus_status_bits"),
        F.row_number().over(w5).alias("seq_no"),
        F.last("humidity_avg_1h", ignorenulls=True).over(w6).alias("last_known_humidity_avg_1h"),
        F.ntile(3).over(w7).alias("pressure_bucket"),
        F.dense_rank().over(w8).alias("humidity_avg_1h_dense_rank"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "running_running_temp_c_plus_status_bits",
        "seq_no",
        F.dense_rank().over(w9).alias("seq_no_dense_rank"),
        F.last("temp_c", ignorenulls=True).over(w4).alias("last_known_temp_c"),
        F.ntile(5).over(w10).alias("corr_temp_c_humidity_bucket"),
    )
)

result = (
    metrics_device_readings
    .select(
        "id",
        "temp_c",
        F.col("last_known_temp_c").alias("LAST_KNOWN_TEMP_C_VAL"),
        F.col("pressure").alias("PRESSURE"),
    )
    .unpivot(
        ids=["id"],
        values=["temp_c", "LAST_KNOWN_TEMP_C_VAL", "PRESSURE"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
