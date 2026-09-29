from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1023, "PREMIUM").otherwise("STANDARD"))
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .cube("pressure_tier", "hour_window")
    .agg(
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.grouping("pressure_tier").alias("gid"),
    )
)
