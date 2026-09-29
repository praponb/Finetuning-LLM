from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.percentile_approx("temp_c", 0.9).alias("temp_c_p90"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles_2"),
        F.corr("humidity", "status_bits").alias("corr_humidity_status_bits"),
    )
    .filter(F.col("corr_humidity_status_bits") < 0)
    .orderBy(F.col("dv_01_count").desc_nulls_first())
)
