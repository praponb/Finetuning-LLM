from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("humidity", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy(F.col("pressure_avg_3_prior").desc_nulls_first())
w_grp = Window.partitionBy("device_code")
w8 = Window.partitionBy("device_code").orderBy(F.col("running_pressure").desc_nulls_first())
w9 = Window.partitionBy("device_code").orderBy("pressure_avg_3_prior_rank")

latest = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("running_pressure", F.sum("pressure").over(w2))
    .withColumn("humidity_avg_2_prior", F.avg("humidity").over(w3))
    .withColumn("pressure_avg_3_prior", F.avg("pressure").over(w4))
)

valid = (
    metrics
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("pressure_avg_3_prior_times_running_pressure", (F.col("pressure_avg_3_prior") * F.col("running_pressure")))
)

metrics_device_readings = (
    valid
    .select(
        "*",
        F.ntile(4).over(w5).alias("temp_c_bucket"),
        F.rank().over(w_hist).alias("pressure_avg_3_prior_rank"),
        F.sum("humidity").over(w_grp).alias("device_code_sum_humidity"),
        F.rank().over(w8).alias("running_pressure_rank"),
    )
)

kept_keys = (
    metrics_device_readings
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") <= 10)
        .select("id")
    )
)

verified = (
    metrics_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("device_code_sum_status_bits", F.sum("status_bits").over(w_grp))
    .withColumn("pressure_avg_3_prior_rank_share", F.round((F.col("pressure_avg_3_prior_rank") / F.sum("pressure_avg_3_prior_rank").over(w_grp)), 4))
    .withColumn("pressure_avg_3_prior_rank_percent_rank", F.percent_rank().over(w9))
)

grouped = (
    enriched
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.percentile_approx("pressure_avg_3_prior_rank_share", [0.25, 0.5, 0.75]).alias("pressure_avg_3_prior_rank_share_quartiles"),
    )
    .filter(F.col("status_bits_bit_and") < 1)
)

result = (
    grouped
    .select(
        "device_code",
        "hour_window",
        "dv_04_count",
        "pressure_avg_3_prior_rank_share_quartiles",
    )
    .orderBy(F.col("device_code").desc())
)
