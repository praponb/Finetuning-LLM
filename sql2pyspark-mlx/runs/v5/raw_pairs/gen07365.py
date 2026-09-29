from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("id", "firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits").between(4, 11))
        .select("id", "firmware")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("device_code")
    .agg(
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("pressure_per_dv_01"),
    )
)
