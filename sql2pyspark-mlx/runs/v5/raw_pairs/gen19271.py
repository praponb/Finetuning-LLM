from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("running_max_humidity", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("running_count_humidity", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("running_count_humidity").asc_nulls_last(), "id")
w6 = Window.partitionBy("device_code").orderBy("running_count_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_count_humidity", "id")
w8 = Window.partitionBy("device_code").orderBy("last3_count_status_bits_times_corr_pressure_humidity", "id")

scored = (
    df_device_readings
    .filter(F.col("status_bits").between(6, 11))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.count("humidity").over(w).alias("running_count_humidity"),
        F.min("humidity").over(w).alias("running_min_humidity"),
        F.max("humidity").over(w).alias("running_max_humidity"),
        F.sum("humidity").over(w).alias("running_sum_humidity"),
        F.corr("pressure", "humidity").over(w2).alias("corr_pressure_humidity"),
    )
)

enriched = (
    scored
    .withColumn("last3_avg_status_bits", F.avg("status_bits").over(w3))
    .withColumn("last3_min_status_bits", F.min("status_bits").over(w3))
    .withColumn("last3_count_status_bits", F.count("status_bits").over(w3))
)

valid = (
    enriched
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("running_count_humidity").isNotNull())
    .select(
        "*",
        (F.col("last3_count_status_bits") * F.col("corr_pressure_humidity")).alias("last3_count_status_bits_times_corr_pressure_humidity"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "temp_c",
        "running_count_humidity",
        "running_sum_humidity",
        "last3_min_status_bits",
        "last3_count_status_bits_times_corr_pressure_humidity",
    )
)

ranked_device_readings = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked_device_readings
    .withColumn("cumulative_last3_count_status_bits_times_corr_pressure_humidity", F.sum("last3_count_status_bits_times_corr_pressure_humidity").over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

result = (
    metrics
    .select(
        "*",
        F.max("last3_count_status_bits_times_corr_pressure_humidity").over(w).alias("running_max_last3_count_status_bits_times_corr_pressure_humidity"),
        F.min("last3_count_status_bits_times_corr_pressure_humidity").over(w).alias("running_min_last3_count_status_bits_times_corr_pressure_humidity"),
        F.count("last3_count_status_bits_times_corr_pressure_humidity").over(w).alias("running_count_last3_count_status_bits_times_corr_pressure_humidity"),
        F.ntile(4).over(w8).alias("last3_count_status_bits_times_corr_pressure_humidity_bucket"),
    )
)
