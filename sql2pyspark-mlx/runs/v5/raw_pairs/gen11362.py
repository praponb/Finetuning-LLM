from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.round("pressure", 2).alias("pressure_rounded"),
    )
)

result = (
    base
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure").isNotNull())
        .select("id")
    )
)
