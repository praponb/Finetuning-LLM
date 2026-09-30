from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 13, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["device_code", "status_bits_tier"], ["device_code"]],
        "device_code",
        "status_bits_tier",
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.grouping("device_code").alias("grouping_id"),
    )
    .orderBy(F.col("avg_status_bits").desc_nulls_last())
)
