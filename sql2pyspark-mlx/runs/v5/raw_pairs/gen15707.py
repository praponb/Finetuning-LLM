from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        "firmware",
        F.stddev_samp("status_bits").over(w).alias("device_code_stddev_samp_status_bits"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_2"),
        F.last("temp_c", ignorenulls=True).over(w_ordered).alias("last_known_temp_c"),
    )
)

filtered = (
    scored
    .filter(F.col("humidity").isNotNull())
    .select(
        "id",
        "firmware",
        "device_code_stddev_samp_status_bits",
        "last_known_temp_c",
        (F.col("last_known_temp_c") * F.col("device_code_stddev_samp_status_bits")).alias("last_known_temp_c_times_device_code_stddev_samp_status_bits"),
    )
)

valid = (
    filtered
    .filter(F.col("device_code_stddev_samp_status_bits").isNotNull())
    .select("*", F.lower("firmware").alias("firmware_lower"))
)

filtered_device_readings = (
    valid
    .filter(F.col("device_code_stddev_samp_status_bits") != 10)
    .select(
        "id",
        "device_code_stddev_samp_status_bits",
        "last_known_temp_c",
        "last_known_temp_c_times_device_code_stddev_samp_status_bits",
        F.coalesce("last_known_temp_c_times_device_code_stddev_samp_status_bits", F.lit(0)).alias("last_known_temp_c_times_device_code_stddev_samp_status_bits_filled"),
    )
)

result = (
    filtered_device_readings
    .withColumn("last_known_temp_c_times_device_code_stddev_samp_status_bits_tier", F.when(F.col("last_known_temp_c_times_device_code_stddev_samp_status_bits") >= 90, "PREMIUM").otherwise("STANDARD"))
    .rollup("last_known_temp_c_times_device_code_stddev_samp_status_bits_tier")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("last_known_temp_c", 0.5).alias("last_known_temp_c_p50"),
        F.percentile_approx("last_known_temp_c", 0.95).alias("last_known_temp_c_p95"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)
