from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["device_code", "activity_day"], ["device_code"]],
        "device_code",
        "activity_day",
    )
    .agg(
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.corr("status_bits", "humidity").alias("corr_status_bits_humidity"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.grouping("device_code").alias("gid"),
    )
    .filter(F.col("temp_c_stddev") > 0)
    .orderBy("device_code")
)
