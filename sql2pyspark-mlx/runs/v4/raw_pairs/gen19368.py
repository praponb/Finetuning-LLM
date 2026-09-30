from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w_grp = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w3 = Window.orderBy("firmware_length")
w4 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure_cume_dist")
w_ordered = Window.orderBy("humidity")
w7 = Window.partitionBy("device_code").orderBy("device_code_stddev_samp_firmware_length_rank", "id").rowsBetween(-2, -1)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.length("firmware").alias("firmware_length"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.corr("temp_c", "pressure").over(w).alias("corr_temp_c_pressure"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
        F.cume_dist().over(w_grp).alias("pressure_cume_dist"),
        F.rank().over(w3).alias("firmware_length_rank"),
    )
)

scored = (
    with_windows
    .withColumn("running_firmware_length_rank", F.sum("firmware_length_rank").over(w4))
    .withColumn("pressure_cume_dist_cume_dist", F.cume_dist().over(w5))
)

scored_device_readings = (
    scored
    .withColumn("device_code_stddev_samp_firmware_length_rank", F.stddev_samp("firmware_length_rank").over(w))
    .withColumn("device_code_stddev_samp_pressure_cume_dist", F.stddev_samp("pressure_cume_dist").over(w))
)

with_windows_device_readings = (
    scored_device_readings
    .withColumn("humidity_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("device_code_stddev_samp_pressure_cume_dist_avg_2_prior", F.avg("device_code_stddev_samp_pressure_cume_dist").over(w7))
)

result = (
    with_windows_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "firmware_length",
        "corr_temp_c_pressure",
        "temp_c_share",
    )
)
