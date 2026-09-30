from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 10, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("humidity_per_dv_04"),
    )
)
