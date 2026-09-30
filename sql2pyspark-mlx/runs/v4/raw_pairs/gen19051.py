from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c_minus_humidity", "id")
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())
w_part = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("burst_id").desc())
w9 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy(F.col("burst_id_rank").desc_nulls_last())
w11 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w12 = Window.partitionBy("device_code")
w13 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w14 = Window.partitionBy("device_code").orderBy("first_known_pressure")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 12:00:00").cast("timestamp")) & (F.col("humidity") > 70))
    .withColumn("temp_c_minus_humidity", (F.col("temp_c") - F.col("humidity")))
)

enriched = (
    valid
    .withColumn("next_temp_c_minus_humidity", F.lead("temp_c_minus_humidity", 1).over(w))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("temp_c_percent_rank", F.percent_rank().over(w4))
    .withColumn("temp_c_minus_humidity_avg_2_prior", F.avg("temp_c_minus_humidity").over(w_part))
)

gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w6).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w6) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w7))

remaining_keys = (
    sessions_device_readings
    .select("firmware")
    .intersect(
        df_device_readings
        .filter(F.col("status_bits") != 11)
        .select("firmware")
    )
)

reconciled = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("burst_id_rank", F.rank().over(w8))
    .withColumn("temp_c_minus_humidity_sum_1h", F.sum("temp_c_minus_humidity").over(w9))
)

with_windows = (
    scored
    .select(
        "*",
        F.cume_dist().over(w10).alias("burst_id_rank_cume_dist"),
        F.avg("first_known_pressure").over(w11).alias("first_known_pressure_avg_1h"),
        F.corr("pressure", "humidity").over(w12).alias("corr_pressure_humidity"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("corr_pressure_humidity").over(w13).alias("corr_pressure_humidity_avg_15m"),
        F.cume_dist().over(w14).alias("first_known_pressure_cume_dist"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("first_known_pressure_cume_dist") >= 0.75, "LARGE").otherwise("SMALL").alias("first_known_pressure_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
        F.min("reading_ts").alias("started_at"),
        F.round(F.avg("burst_id_rank"), 2).alias("avg_burst_id_rank"),
    )
    .orderBy(F.col("burst_id_p90").desc())
)
