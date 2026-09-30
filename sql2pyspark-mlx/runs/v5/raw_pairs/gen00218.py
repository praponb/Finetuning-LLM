from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.when(F.col("firmware").isin(["1.0.3", "1.1.0"]), "Group A").otherwise("Other").alias("firmware_group"),
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

result = (
    valid
    .select("id")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits") > 9)
        .select("id")
    )
)
