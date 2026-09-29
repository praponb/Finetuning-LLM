from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 12:00:00"))) & F.col("device_code").isin(["DV-02", "DV-04"]))
    .withColumn("temp_c_minus_status_bits", (F.col("temp_c") - F.col("status_bits")))
    .withColumn("firmware_title", F.initcap(F.trim("firmware")))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    sessions_device_readings
    .select(
        "*",
        F.sum("temp_c").over(w3).alias("running_temp_c"),
        F.sum("temp_c").over(w4).alias("cumulative_temp_c"),
        F.sum("humidity").over(w_part).alias("humidity_sum_15m"),
    )
)

valid_device_readings = (
    enriched
    .filter(F.col("humidity") >= 48)
    .select(
        "*",
        (F.col("humidity_sum_15m") + F.col("temp_c")).alias("humidity_sum_15m_plus_temp_c"),
    )
)

ranked = (
    valid_device_readings
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

stats = (
    ranked
    .withColumn("temp_c_minus_status_bits_tier", F.when(F.col("temp_c_minus_status_bits") >= 89, "PREMIUM").otherwise("STANDARD"))
    .rollup("temp_c_minus_status_bits_tier", "device_code")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("running_temp_c")).otherwise(0)).alias("dv_01_running_temp_c"),
        F.min("reading_ts").alias("first_seen"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity_sum_15m_plus_temp_c")).otherwise(0)).alias("dv_04_humidity_sum_15m_plus_temp_c"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("humidity_per_dv_03"),
        F.percentile_approx("humidity", 0.95).alias("humidity_p95"),
        F.grouping("temp_c_minus_status_bits_tier").alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "temp_c_minus_status_bits_tier",
        "first_seen",
        "dv_04_humidity_sum_15m_plus_temp_c",
        "grouping_level",
    )
    .orderBy(F.col("grouping_level").desc())
)
