from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code")
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("pressure") > 993)
    .withColumn("greatest_status_bits_temp_c", F.greatest("status_bits", "temp_c"))
)

kept_keys = (
    cleaned
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") <= 62)
        .select("id", "device_code")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("reading_ts") > F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("device_code") == "DV-01", 1).otherwise(0).alias("is_dv_01"),
    )
)

enriched = (
    base
    .select(
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "greatest_status_bits_temp_c",
        "is_dv_01",
        F.max("greatest_status_bits_temp_c").over(w_part).alias("device_code_max_greatest_status_bits_temp_c"),
        F.sum("pressure").over(w2).alias("pressure_sum_2h"),
        F.avg("pressure").over(w_part).alias("device_code_avg_pressure"),
        F.round((F.col("humidity") / F.sum("humidity").over(w_part)), 4).alias("humidity_share"),
        F.count("pressure").over(w_running).alias("pressure_count_2h"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("device_code_max_greatest_status_bits_temp_c") >= 79, "PREMIUM").otherwise("STANDARD").alias("device_code_max_greatest_status_bits_temp_c_tier"),
        F.when(F.col("humidity") >= 81, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.round(F.avg("is_dv_01"), 2).alias("avg_is_dv_01"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.percentile_approx("temp_c", 0.9).alias("temp_c_p90"),
    )
    .orderBy(F.col("avg_is_dv_01").desc())
)
