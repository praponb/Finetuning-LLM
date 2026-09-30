from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("corr_temp_c_humidity_2").desc())
w7 = Window.partitionBy("device_code").orderBy(F.col("corr_status_bits_humidity").asc_nulls_last())
w8 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

matched_keys = (
    df_device_readings
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.to_date("reading_ts") < "2024-01-11")
        .select("firmware", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.corr("temp_c", "humidity").over(w_part).alias("corr_temp_c_humidity"),
        F.count("temp_c").over(w2).alias("temp_c_count_30m"),
        F.count("pressure").over(w_part).alias("device_code_count_pressure"),
        F.corr("status_bits", "humidity").over(w_part).alias("corr_status_bits_humidity"),
        F.corr("temp_c", "humidity").over(w_part).alias("corr_temp_c_humidity_2"),
    )
)

with_windows_device_readings = (
    with_windows
    .withColumn("device_code_count_pressure_sum_2h", F.sum("device_code_count_pressure").over(w3))
    .withColumn("device_code_min_corr_status_bits_humidity", F.min("corr_status_bits_humidity").over(w_part))
    .withColumn("pressure_rank", F.rank().over(w4))
    .withColumn("status_bits_sum_1h", F.sum("status_bits").over(w5))
    .withColumn("corr_temp_c_humidity_2_cume_dist", F.cume_dist().over(w6))
)

valid = (
    with_windows_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 16:00:00"))) & F.col("temp_c_count_30m").between(9, 14))
    .select(
        "*",
        (F.col("device_code_count_pressure") - F.col("device_code_count_pressure_sum_2h")).alias("device_code_count_pressure_minus_device_code_count_pressure_sum_2h"),
    )
)

valid_device_readings = (
    valid
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 15:00:00").cast("timestamp")) & (F.col("device_code") == "DV-01"))
    .select(
        "*",
        (F.col("device_code_min_corr_status_bits_humidity") - F.col("temp_c_count_30m")).alias("device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m"),
    )
)

enriched = (
    valid_device_readings
    .select(
        "*",
        F.dense_rank().over(w7).alias("corr_status_bits_humidity_dense_rank"),
        F.round((F.col("pressure") / F.sum("pressure").over(w_part)), 4).alias("pressure_share"),
        F.count("device_code_count_pressure_minus_device_code_count_pressure_sum_2h").over(w8).alias("device_code_count_pressure_minus_device_code_count_pressure_sum_2h_count_2h"),
        F.sum("device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m").over(w3).alias("device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m_sum_2h"),
    )
)

base = (
    enriched
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 16:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "corr_temp_c_humidity",
        "temp_c_count_30m",
        "device_code_count_pressure",
        "corr_status_bits_humidity",
        "corr_temp_c_humidity_2",
        "device_code_count_pressure_sum_2h",
        "corr_temp_c_humidity_2_cume_dist",
        "device_code_count_pressure_minus_device_code_count_pressure_sum_2h",
        "device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m",
        "pressure_share",
        "device_code_count_pressure_minus_device_code_count_pressure_sum_2h_count_2h",
        "device_code_min_corr_status_bits_humidity_minus_temp_c_count_30m_sum_2h",
        (F.col("corr_status_bits_humidity") * F.col("temp_c_count_30m")).alias("corr_status_bits_humidity_times_temp_c_count_30m"),
    )
)

summary = (
    base
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .withColumn("corr_temp_c_humidity_2_tier", F.when(F.col("corr_temp_c_humidity_2") >= 0.52, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["device_code", "hour_window", "corr_temp_c_humidity_2_tier"], ["device_code", "hour_window"], []],
        "device_code",
        "hour_window",
        "corr_temp_c_humidity_2_tier",
    )
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.round(F.avg("device_code_count_pressure_sum_2h"), 2).alias("avg_device_code_count_pressure_sum_2h"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.avg("corr_status_bits_humidity_times_temp_c_count_30m"), 2).alias("avg_corr_status_bits_humidity_times_temp_c_count_30m"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    summary
    .select(
        "hour_window",
        "corr_temp_c_humidity_2_tier",
        "first_seen",
        "dv_02_events",
        "avg_device_code_count_pressure_sum_2h",
        "device_code_list",
        "avg_corr_status_bits_humidity_times_temp_c_count_30m",
    )
)
