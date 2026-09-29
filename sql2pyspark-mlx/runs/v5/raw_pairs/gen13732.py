from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity_adjusted_max_2h").desc_nulls_last())
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

matched_keys = (
    df_device_readings
    .select("id", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(10, 16))
        .select("id", "device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp")) & (F.col("status_bits") == 5))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("humidity") * 0.85), 2).alias("humidity_adjusted"),
    )
)

with_windows = (
    base
    .filter(F.col("device_code").isin(["DV-03", "DV-02"]))
    .select(
        "*",
        F.sum("pressure").over(w).alias("pressure_sum_30m"),
        F.max("humidity_adjusted").over(w2).alias("humidity_adjusted_max_2h"),
        F.count("temp_c").over(w_part).alias("temp_c_count_30m"),
        F.corr("pressure", "status_bits").over(w_grp).alias("corr_pressure_status_bits"),
        F.count("temp_c").over(w_grp).alias("device_code_count_temp_c"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.count("humidity_adjusted").over(w_part).alias("humidity_adjusted_count_30m"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w_grp)), 4).alias("temp_c_share"),
        F.rank().over(w5).alias("humidity_adjusted_max_2h_rank"),
        F.avg("temp_c_count_30m").over(w).alias("temp_c_count_30m_avg_30m"),
    )
)

filtered = (
    metrics
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .withColumn("temp_c_minus_pressure_sum_30m", (F.col("temp_c") - F.col("pressure_sum_30m")))
)

base_device_readings = (
    filtered
    .filter(F.col("temp_c_minus_pressure_sum_30m") > 55)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_adjusted",
        "pressure_sum_30m",
        "humidity_adjusted_max_2h",
        "temp_c_count_30m",
        "corr_pressure_status_bits",
        "device_code_count_temp_c",
        "humidity_adjusted_count_30m",
        "temp_c_share",
        "temp_c_count_30m_avg_30m",
        "temp_c_minus_pressure_sum_30m",
        F.when(F.col("pressure") >= 1028, "High").when(F.col("pressure") >= 1003, "Medium").otherwise("Low").alias("pressure_level"),
        F.round((F.col("pressure") * 1.1), 2).alias("pressure_adjusted"),
    )
)

enriched = (
    base_device_readings
    .withColumn("status_bits_avg_15m", F.avg("status_bits").over(w6))
    .withColumn("device_code_count_pressure_adjusted", F.count("pressure_adjusted").over(w_grp))
    .withColumn("humidity_adjusted_max_30m", F.max("humidity_adjusted").over(w))
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_sum_30m",
        "humidity_adjusted_max_2h",
    )
    .orderBy(F.col("reading_ts").asc_nulls_first())
)
