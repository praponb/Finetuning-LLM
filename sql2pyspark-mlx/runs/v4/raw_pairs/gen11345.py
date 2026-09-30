from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("humidity") <= 56)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.year("reading_ts").alias("reading_ts_year"),
        F.round("pressure", 2).alias("pressure_rounded"),
    )
)

result = (
    filtered
    .select("id", "device_code", "temp_c", "pressure_rounded", "pressure")
    .unpivot(
        ["id", "device_code"],
        ["temp_c", "pressure_rounded", "pressure"],
        "kpi",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
