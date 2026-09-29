from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w_grp = Window.orderBy(F.col("pressure").asc_nulls_first())
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("pressure").asc_nulls_last())
w8 = Window.orderBy(F.col("temp_c_count_1h").desc_nulls_first())
w9 = Window.orderBy(F.col("pressure").desc_nulls_first())
w10 = Window.orderBy(F.col("pressure_rank").asc_nulls_first())
w11 = Window.orderBy(F.col("temp_c_max_15m").desc_nulls_first())
w12 = Window.orderBy(F.col("pressure_rank").desc_nulls_last())
w13 = Window.orderBy(F.col("temp_c_count_1h_cume_dist").desc_nulls_first())

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_str", F.col("status_bits").cast("string"))
    .withColumn("firmware_lower", F.lower("firmware"))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    deduped
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") == "DV-03")
        .select("device_code", "firmware")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "pressure",
        "status_bits",
        "status_bits_str",
        "firmware_lower",
        F.dense_rank().over(w_grp).alias("pressure_dense_rank"),
        F.sum("humidity").over(w3).alias("device_code_sum_humidity"),
        F.count("temp_c").over(w4).alias("temp_c_count_1h"),
        F.max("temp_c").over(w5).alias("temp_c_max_15m"),
        F.max("status_bits").over(w6).alias("status_bits_max_1h"),
    )
)

cleaned_device_readings = (
    scored
    .filter(F.col("pressure") > 1020)
    .select(
        "pressure",
        "status_bits",
        "status_bits_str",
        "firmware_lower",
        "pressure_dense_rank",
        "device_code_sum_humidity",
        "temp_c_count_1h",
        "temp_c_max_15m",
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

base = (
    cleaned_device_readings
    .filter(F.col("temp_c_max_15m").isNull())
    .select(
        "pressure",
        "status_bits",
        "pressure_dense_rank",
        "temp_c_count_1h",
        "temp_c_max_15m",
        F.round("temp_c_count_1h", 0).alias("temp_c_count_1h_rounded"),
    )
)

with_windows = (
    base
    .withColumn("pressure_rank", F.rank().over(w7))
    .withColumn("temp_c_count_1h_cume_dist", F.cume_dist().over(w8))
    .withColumn("pressure_cume_dist", F.cume_dist().over(w9))
)

result = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w10).alias("pressure_rank_cume_dist"),
        F.dense_rank().over(w11).alias("temp_c_max_15m_dense_rank"),
        F.dense_rank().over(w11).alias("temp_c_max_15m_dense_rank_2"),
        F.dense_rank().over(w12).alias("pressure_rank_dense_rank"),
        F.rank().over(w13).alias("temp_c_count_1h_cume_dist_rank"),
    )
)
