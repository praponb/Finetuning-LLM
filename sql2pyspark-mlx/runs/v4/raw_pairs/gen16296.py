from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy(F.col("pressure").asc_nulls_first())

remaining_keys = (
    df_device_readings
    .select("firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c") <= 22)
        .select("firmware")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("corr_pressure_temp_c", F.corr("pressure", "temp_c").over(w))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w)), 4))
    .withColumn("pressure_rank", F.rank().over(w2))
)

cleaned = (
    enriched
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 15:00:00").cast("timestamp")) & F.col("device_code").isin(["DV-01", "DV-04"]))
    .select(
        "id",
        "status_bits",
        "firmware",
        "corr_pressure_temp_c",
        "humidity_share",
        "pressure_rank",
        F.round("humidity_share", 2).alias("humidity_share_rounded"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("status_bits") >= 13, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
    )
)
