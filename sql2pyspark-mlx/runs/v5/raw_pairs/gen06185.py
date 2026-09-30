from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("pressure")
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure_dense_rank").desc_nulls_first())
w4 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .withColumn("device_code_title", F.initcap(F.trim("device_code")))
    .withColumn("device_code_prefix", F.substring("device_code", 1, 2))
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("temp_c").over(w_hist).alias("running_sum_temp_c"),
        F.avg("temp_c").over(w_hist).alias("running_avg_temp_c"),
        F.count("temp_c").over(w_hist).alias("running_count_temp_c"),
        F.dense_rank().over(w_ordered).alias("pressure_dense_rank"),
    )
)

remaining_keys = (
    enriched
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(~F.col("device_code").isin(["DV-03", "DV-04", "DV-02"]))
        .select("id", "device_code")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "device_code",
        "reading_ts",
        "status_bits",
        "device_code_title",
        "device_code_prefix",
        "running_sum_temp_c",
        "running_count_temp_c",
        "pressure_dense_rank",
        F.percent_rank().over(w3).alias("pressure_dense_rank_percent_rank"),
        F.avg("pressure_dense_rank").over(w4).alias("device_code_avg_pressure_dense_rank"),
        F.stddev_samp("running_count_temp_c").over(w4).alias("device_code_stddev_samp_running_count_temp_c"),
    )
)

result = (
    metrics
    .select(
        "device_code",
        "reading_ts",
        "status_bits",
        "device_code_title",
        "running_count_temp_c",
        "pressure_dense_rank",
        "pressure_dense_rank_percent_rank",
        "device_code_avg_pressure_dense_rank",
        "device_code_stddev_samp_running_count_temp_c",
    )
    .orderBy(F.col("pressure_dense_rank").desc())
)
