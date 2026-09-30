from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .withColumn("device_code_title", F.initcap(F.trim("device_code")))
)

result = (
    valid
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("pressure_per_dv_04"),
    )
    .filter(F.col("pressure_per_dv_04") > 0)
)
