from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("session_no").desc())
w_running = Window.orderBy("burst_id", "id")
w_grp = Window.partitionBy("device_code").orderBy(F.col("session_no").desc(), "id")
w8 = Window.partitionBy("device_code")
w9 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-03", "DV-02"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.when(F.col("humidity") >= 65, "High").when(F.col("humidity") >= 47, "Medium").otherwise("Low").alias("humidity_level"),
        F.concat("firmware", F.lit(", "), "device_code").alias("firmware_device_code"),
    )
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

with_gaps_2 = (
    sessions_device_readings
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts") - F.lag("prev_reading_ts").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_device_readings_2 = (
    with_gaps_2
    .select(
        "id",
        "device_code",
        "humidity",
        "prev_reading_ts",
        "new_group_flag",
        "session_no",
        F.sum("is_new_burst").over(w4).alias("burst_id"),
    )
)

metrics = (
    sessions_device_readings_2
    .withColumn("session_no_cume_dist", F.cume_dist().over(w5))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

scored = (
    metrics
    .select(
        "id",
        "humidity",
        "prev_reading_ts",
        "session_no_cume_dist",
        "seq_no",
        "row_seq",
        F.min("seq_no").over(w8).alias("device_code_min_seq_no"),
        F.sum("seq_no").over(w9).alias("running_seq_no"),
        F.round((F.col("humidity") / F.sum("humidity").over(w8)), 4).alias("humidity_share"),
    )
)

filtered = (
    scored
    .filter(F.col("humidity_share").between(0.58, 0.67))
    .select("*", F.coalesce("running_seq_no", F.lit(0)).alias("running_seq_no_filled"))
)

metrics_device_readings = (
    filtered
    .select(
        "*",
        F.sum("row_seq").over(w10).alias("running_sum_row_seq"),
        F.max("row_seq").over(w10).alias("running_max_row_seq"),
        F.min("row_seq").over(w10).alias("running_min_row_seq"),
    )
)

result = (
    metrics_device_readings
    .select(
        "id",
        "humidity",
        "row_seq",
        "device_code_min_seq_no",
        "running_seq_no",
        "humidity_share",
        "running_seq_no_filled",
        "running_sum_row_seq",
        "running_max_row_seq",
    )
    .orderBy(F.col("running_seq_no_filled").desc())
)
