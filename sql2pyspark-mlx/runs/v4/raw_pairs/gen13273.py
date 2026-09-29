from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.when(F.col("device_code").isin(["DV-01", "DV-04"]), "Tier 1").otherwise("Other").alias("device_code_group"),
        F.round((F.col("status_bits") * 0.85), 2).alias("status_bits_adjusted"),
    )
)

result = (
    filtered
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.1.0")
        .select("firmware", "device_code")
    )
)
