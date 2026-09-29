from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("prev_prev_reading_ts", "id")
w6 = Window.orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("seq_no", "id")
w9 = Window.orderBy(F.col("prev_prev_reading_ts").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_part = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

prev_events = islands.withColumn("prev_prev_reading_ts", F.lag("prev_reading_ts").over(w3))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_prev_reading_ts").isNull() | (F.col("prev_reading_ts") > F.col("prev_prev_reading_ts") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w4))

with_windows = (
    sessions_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "prev_prev_reading_ts",
        "session_start_flag",
        "burst_id",
        F.lead("humidity").over(w_ordered).alias("next_humidity"),
        F.lead("new_group_flag").over(w_ordered).alias("next_new_group_flag"),
        F.lag("new_group_flag").over(w_ordered).alias("prev_new_group_flag"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "pressure",
        "prev_prev_reading_ts",
        "session_start_flag",
        "next_humidity",
        "seq_no",
        F.sum("session_start_flag").over(w6).alias("cumulative_session_start_flag"),
        F.first("session_start_flag", ignorenulls=True).over(w7).alias("first_known_session_start_flag"),
    )
)

scored = (
    metrics
    .filter(F.col("first_known_session_start_flag") != 0)
    .withColumn("next_pressure", F.lead("pressure").over(w8))
    .withColumn("prev_pressure", F.lag("pressure").over(w8))
    .withColumn("next_first_known_session_start_flag", F.lead("first_known_session_start_flag").over(w8))
    .withColumn("prev_first_known_session_start_flag", F.lag("first_known_session_start_flag").over(w8))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("seq_no_sum_30d", F.sum("seq_no").over(w9))
)

with_windows_device_readings = (
    scored
    .withColumn("running_count_seq_no_sum_30d", F.count("seq_no_sum_30d").over(w_part))
    .withColumn("running_max_seq_no_sum_30d", F.max("seq_no_sum_30d").over(w_part))
    .withColumn("running_sum_seq_no_sum_30d", F.sum("seq_no_sum_30d").over(w_part))
)

result = (
    with_windows_device_readings
    .groupBy(
        F.date_trunc("YEAR", "prev_prev_reading_ts").alias("year_start"),
        F.when(F.col("seq_no_sum_30d") >= 35, "LARGE").otherwise("SMALL").alias("seq_no_sum_30d_tier"),
        F.date_trunc("MONTH", "prev_prev_reading_ts").alias("activity_month"),
    )
    .agg(
        F.max("prev_prev_reading_ts").alias("last_prev_prev_reading_ts"),
        F.percentile_approx("next_humidity", [0.25, 0.5, 0.75]).alias("next_humidity_quartiles"),
    )
    .orderBy(F.col("seq_no_sum_30d_tier").asc_nulls_last())
)
