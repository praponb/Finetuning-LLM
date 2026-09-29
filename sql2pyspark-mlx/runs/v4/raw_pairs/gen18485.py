from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("burst_id_2").desc(), "id")
w8 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_device_readings
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w4))

with_prev = islands.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands_device_readings = with_gaps.withColumn("burst_id_2", F.sum("is_new_burst").over(w4))

enriched = (
    islands_device_readings
    .withColumn("running_event_seq", F.sum("event_seq").over(w5))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w8))
)

base = (
    enriched
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 11:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_cume_dist",
        "event_seq",
        "burst_id",
        "is_new_burst",
        "row_seq",
        F.least("is_new_burst", "running_event_seq").alias("least_is_new_burst_running_event_seq"),
    )
)

wide = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-03", "DV-02"])
    .agg(F.avg("burst_id").alias("value_sum"))
    .select(
        "firmware",
        F.col("DV-04").alias("dv_04"),
        F.col("DV-03").alias("dv_03"),
        F.col("DV-02").alias("dv_02"),
    )
)

result = (
    wide
    .select("firmware", "dv_04", "dv_03")
    .orderBy(F.col("dv_03").desc())
)
