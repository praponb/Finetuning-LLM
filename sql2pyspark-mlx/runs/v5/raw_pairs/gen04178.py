from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("firmware").isin(["2.0.1", "1.0.3"]))
    .withColumn("device_code_upper", F.upper("device_code"))
)

result = (
    cleaned
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03", "DV-02"])
    .agg(F.max("temp_c"))
    .select(
        "firmware",
        F.col("DV-01").alias("dv_01"),
        F.col("DV-03").alias("dv_03"),
        F.col("DV-02").alias("dv_02"),
    )
)
