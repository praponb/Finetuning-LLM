from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.to_date("reading_ts") < "2025-09-01")
        .select("firmware", "device_code")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.min("pressure").alias("min_pressure"),
    )
    .orderBy(F.col("min_pressure").desc())
)
