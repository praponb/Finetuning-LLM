from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 37.4, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c")).otherwise(0)).alias("dv_04_temp_c"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("humidity")).otherwise(0)).alias("dv_03_humidity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("corr_pressure_status_bits") < 0)
)
