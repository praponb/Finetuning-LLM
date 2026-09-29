from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("greatest_status_bits_temp_c", F.greatest("status_bits", "temp_c"))
)

result = (
    valid
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        "greatest_status_bits_temp_c",
        "pressure",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY", "greatest_status_bits_temp_c", "pressure"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
