from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 9, "HIGH").otherwise("LOW").alias("status_bits_tier"),
        "device_code",
    )
    .agg(
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
        F.count("*").alias("event_count"),
    )
)
