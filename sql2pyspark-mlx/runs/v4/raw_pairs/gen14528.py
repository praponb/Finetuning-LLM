from pyspark.sql import functions as F

result = (
    df_device_readings
    .rollup(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("status_bits") >= 10, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
    )
)
