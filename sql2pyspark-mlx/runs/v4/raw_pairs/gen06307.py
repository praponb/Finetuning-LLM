from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code")
w_grp_2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("device_code_count_pressure").desc_nulls_first())
w6 = Window.partitionBy("device_code").orderBy("device_code_count_pressure")

remaining_keys = (
    df_device_readings
    .select("device_code", "id")
    .intersect(
        df_device_readings
        .filter(F.col("status_bits") != 6)
        .select("device_code", "id")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.corr("temp_c", "humidity").over(w_grp).alias("corr_temp_c_humidity"),
        F.avg("temp_c").over(w_grp_2).alias("temp_c_avg_15m"),
        F.count("pressure").over(w_grp).alias("device_code_count_pressure"),
        F.round((F.col("pressure") / F.sum("pressure").over(w_grp)), 4).alias("pressure_share"),
    )
)

enriched = (
    scored
    .withColumn("pressure_cume_dist", F.cume_dist().over(w3))
    .withColumn("temp_c_avg_15m_2", F.avg("temp_c").over(w4))
    .withColumn("device_code_count_pressure_dense_rank", F.dense_rank().over(w5))
    .withColumn("device_code_count_pressure_rank", F.rank().over(w6))
)

valid = (
    enriched
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & F.col("temp_c_avg_15m").between(33, 39))
    .select("*", F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"))
)

valid_device_readings = (
    valid
    .filter(F.col("is_2_0_1") == 74)
    .select(
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "corr_temp_c_humidity",
        "temp_c_avg_15m",
        "device_code_count_pressure",
        "pressure_share",
        "pressure_cume_dist",
        "temp_c_avg_15m_2",
        "device_code_count_pressure_dense_rank",
        "device_code_count_pressure_rank",
        "is_2_0_1",
        (F.col("pressure") * F.col("device_code_count_pressure_dense_rank")).alias("pressure_times_device_code_count_pressure_dense_rank"),
    )
)

result = (
    valid_device_readings
    .groupBy(
        F.when(F.col("temp_c_avg_15m") >= 35, "HIGH").otherwise("LOW").alias("temp_c_avg_15m_tier"),
    )
    .agg(
        F.round(F.avg("is_2_0_1"), 2).alias("avg_is_2_0_1"),
        F.percentile_approx("temp_c_avg_15m_2", [0.25, 0.5, 0.75]).alias("temp_c_avg_15m_2_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("avg_is_2_0_1").desc())
)
