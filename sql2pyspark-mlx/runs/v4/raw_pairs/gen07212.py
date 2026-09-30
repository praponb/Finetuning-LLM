from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select("*", F.col("reading_ts").cast("date").alias("reading_ts_day"))
)

kept_keys = (
    filtered
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("pressure").isNull())
        .select("device_code", "firmware")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w).alias("pressure_dense_rank"),
        F.sum("status_bits").over(w2).alias("status_bits_sum_2h"),
        F.corr("pressure", "temp_c").over(w3).alias("corr_pressure_temp_c"),
    )
)

scored = (
    with_windows
    .withColumn("status_bits_sum_2h_count_2h", F.count("status_bits_sum_2h").over(w2))
    .withColumn("device_code_sum_humidity", F.sum("humidity").over(w3))
    .withColumn("corr_pressure_temp_c_sum_30m", F.sum("corr_pressure_temp_c").over(w4))
)

result = (
    scored
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "reading_ts_day",
        "pressure_dense_rank",
        "status_bits_sum_2h",
    )
)
