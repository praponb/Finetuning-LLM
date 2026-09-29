from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .withColumn("status_bits_num", F.col("status_bits").cast("string"))
    .withColumn("humidity_whole", F.floor("humidity"))
)

result = (
    filtered
    .groupBy("device_code")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("status_bits", 0.9).alias("status_bits_p90"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity"))).alias("dv_02_total_humidity"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
    )
    .filter(F.col("dv_02_total_humidity") > 0)
    .orderBy(F.col("dv_02_total_humidity").desc())
)
