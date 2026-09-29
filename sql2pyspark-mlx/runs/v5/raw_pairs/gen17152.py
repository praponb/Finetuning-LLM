from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") <= 6)
        .select("device_code", "firmware")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("device_code")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("pressure")).otherwise(0)).alias("dv_02_pressure"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    )
)
