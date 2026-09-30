from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy(F.col("is_new_burst").asc_nulls_last())
w_running = Window.partitionBy("device_code")
w_ordered = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("island_id_plus_status_bits")
w8 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first())
w9 = Window.partitionBy("device_code").orderBy(F.col("corr_pressure_humidity").desc_nulls_first())

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

kept_keys = (
    sessions_device_readings
    .select("device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c") < 26)
        .select("device_code")
    )
)

reconciled = (
    sessions_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("is_new_burst_cume_dist"),
        F.stddev_samp("island_id").over(w_running).alias("device_code_stddev_samp_island_id"),
        F.round((F.col("pressure") / F.sum("pressure").over(w_running)), 4).alias("pressure_share"),
        F.sum("is_new_burst").over(w_ordered).alias("is_new_burst_sum_1h"),
        F.corr("pressure", "humidity").over(w_running).alias("corr_pressure_humidity"),
    )
)

valid = (
    metrics
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        (F.col("is_new_burst_sum_1h") / F.nullif("status_bits", F.lit(0))).alias("is_new_burst_sum_1h_per_status_bits"),
        (F.col("island_id") + F.col("status_bits")).alias("island_id_plus_status_bits"),
    )
)

enriched = (
    valid
    .withColumn("device_code_min_island_id", F.min("island_id").over(w_running))
    .withColumn("device_code_count_humidity", F.count("humidity").over(w_running))
)

with_windows = (
    enriched
    .withColumn("pressure_share_max_30m", F.max("pressure_share").over(w6))
    .withColumn("island_id_plus_status_bits_rank", F.rank().over(w7))
)

metrics_device_readings = (
    with_windows
    .withColumn("pressure_dense_rank", F.dense_rank().over(w8))
    .withColumn("pressure_sum_30m", F.sum("pressure").over(w6))
    .withColumn("corr_pressure_humidity_cume_dist", F.cume_dist().over(w9))
    .withColumn("device_code_stddev_samp_device_code_stddev_samp_island_id", F.stddev_samp("device_code_stddev_samp_island_id").over(w_running))
    .withColumn("device_code_min_island_id_2", F.min("island_id").over(w_running))
)

result = (
    metrics_device_readings
    .select(
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        "island_id",
        "is_new_burst_cume_dist",
        "pressure_share",
        "corr_pressure_humidity",
        "is_new_burst_sum_1h_per_status_bits",
    )
)
