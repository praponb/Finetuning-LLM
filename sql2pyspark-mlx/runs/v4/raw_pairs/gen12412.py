from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select("*", F.lower("device_code").alias("device_code_lower"))
)

result = (
    cleaned
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 9, "LARGE").otherwise("SMALL"))
    .cube("status_bits_tier", "device_code")
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.percentile_approx("status_bits", 0.95).alias("status_bits_p95"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("status_bits_bit_and")
)
