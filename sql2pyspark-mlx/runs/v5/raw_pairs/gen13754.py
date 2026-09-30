from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp")) & F.col("firmware").isin(["1.0.3", "1.1.0"]))
    .select("*", F.round((F.col("humidity") * 1.1), 2).alias("humidity_adjusted"))
)

result = (
    cleaned
    .select("id", "device_code", "humidity_adjusted", "pressure", "humidity", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity_adjusted", "pressure", "humidity", "temp_c"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
