from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("humidity") >= 71, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
    )
    .filter(F.col("corr_status_bits_pressure") < 1000000)
)
