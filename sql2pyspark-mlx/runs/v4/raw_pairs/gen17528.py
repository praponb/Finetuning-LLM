from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("humidity_rounded")
w_running = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("corr_pressure_status_bits").asc_nulls_first())

base = (
    df_device_readings
    .filter(F.col("temp_c") >= 28)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("humidity", 2).alias("humidity_rounded"),
    )
)

remaining_keys = (
    base
    .select("id", "firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits") == 10)
        .select("id", "firmware")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("firmware", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("reading_ts") < "2023-07-01")
        .select("firmware", "device_code")
    )
)

kept_rows_device_readings = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows_device_readings
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("humidity_rounded_dense_rank"),
        F.corr("humidity", "pressure").over(w_running).alias("corr_humidity_pressure"),
        F.corr("pressure", "status_bits").over(w_running).alias("corr_pressure_status_bits"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.stddev_samp("humidity_rounded_dense_rank").over(w_running).alias("device_code_stddev_samp_humidity_rounded_dense_rank"),
        F.avg("corr_humidity_pressure").over(w_running).alias("device_code_avg_corr_humidity_pressure"),
        F.min("status_bits").over(w_running).alias("device_code_min_status_bits"),
    )
)

filtered = (
    with_windows
    .filter(F.col("corr_humidity_pressure") > -0.1)
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "humidity_rounded",
        "corr_humidity_pressure",
        "corr_pressure_status_bits",
        "device_code_stddev_samp_humidity_rounded_dense_rank",
        "device_code_avg_corr_humidity_pressure",
        (F.col("pressure") * F.col("humidity_rounded_dense_rank")).alias("pressure_times_humidity_rounded_dense_rank"),
    )
)

metrics = (
    filtered
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w_running))
    .withColumn("device_code_avg_corr_pressure_status_bits", F.avg("corr_pressure_status_bits").over(w_running))
    .withColumn("device_code_sum_status_bits", F.sum("status_bits").over(w_running))
    .withColumn("corr_pressure_status_bits_rank", F.rank().over(w3))
)

grouped = (
    metrics
    .groupBy("device_code")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
)

result = (
    grouped
    .select("device_code", "distinct_ids", "device_code_list")
    .orderBy(F.col("device_code").desc_nulls_first())
)
