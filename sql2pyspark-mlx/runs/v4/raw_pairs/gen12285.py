from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("pressure"))).alias("dv_01_avg_pressure"),
    )
    .orderBy(F.col("dv_03_rows").asc_nulls_first())
)
