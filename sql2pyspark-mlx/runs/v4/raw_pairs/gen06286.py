from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code").orderBy("pressure_sum_15m", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("device_code")
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("temp_c_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("session_id_2", "id").rowsBetween(-3, -1)
w12 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w13 = Window.orderBy(F.col("device_code_max_session_id_2").desc())
w14 = Window.partitionBy("device_code").orderBy(F.col("running_session_id_2").desc(), "id")

enriched = (
    df_device_readings
    .withColumn("humidity_count_2h", F.count("humidity").over(w))
    .withColumn("pressure_sum_15m", F.sum("pressure").over(w2))
)

scored = (
    enriched
    .withColumn("temp_c_bucket", F.ntile(5).over(w3))
    .withColumn("pressure_avg_6_prior", F.avg("pressure").over(w4))
    .withColumn("temp_c_avg_2_prior", F.avg("temp_c").over(w_recent))
    .withColumn("pressure_sum_15m_share", F.round((F.col("pressure_sum_15m") / F.sum("pressure_sum_15m").over(w6)), 4))
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w7) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w8))

flagged = (
    islands
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w7) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands_device_readings = flagged.withColumn("session_id_2", F.sum("is_new_session").over(w8))

with_windows = (
    islands_device_readings
    .withColumn("running_temp_c_avg_2_prior", F.sum("temp_c_avg_2_prior").over(w9))
    .withColumn("running_session_id_2", F.sum("session_id_2").over(w10))
    .withColumn("pressure_avg_3_prior", F.avg("pressure").over(w11))
)

prev_events = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w7))

with_gaps_2 = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps_2.withColumn("burst_id", F.sum("session_start_flag").over(w8))

with_windows_device_readings = (
    sessions_device_readings
    .withColumn("device_code_min_temp_c", F.min("temp_c").over(w6))
    .withColumn("device_code_max_session_id_2", F.max("session_id_2").over(w6))
)

result = (
    with_windows_device_readings
    .withColumn("pressure_avg_6_prior_max_30m", F.max("pressure_avg_6_prior").over(w12))
    .withColumn("device_code_stddev_samp_humidity_count_2h", F.stddev_samp("humidity_count_2h").over(w6))
    .withColumn("last_known_humidity_count_2h", F.last("humidity_count_2h", ignorenulls=True).over(w8))
    .withColumn("device_code_max_session_id_2_percent_rank", F.percent_rank().over(w13))
    .withColumn("running_session_id_2_bucket", F.ntile(4).over(w14))
)
