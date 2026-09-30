from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("next_temp_c").desc_nulls_first(), "id")
w4 = Window.partitionBy("device_code").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())
w7 = Window.partitionBy("device_code").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w9 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 08:00:00")))
    .select("*", F.least("pressure", "status_bits").alias("least_pressure_status_bits"))
)

scored = (
    filtered
    .withColumn("next_temp_c", F.lead("temp_c").over(w))
    .withColumn("prev_temp_c", F.lag("temp_c").over(w))
    .withColumn("next_pressure", F.lead("pressure").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .withColumn("last_known_next_pressure", F.last("next_pressure", ignorenulls=True).over(w4))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w5))
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w6))
    .withColumn("row_seq_2", F.row_number().over(w7))
)

prev_events = enriched.withColumn("prev_reading_ts", F.lag("reading_ts").over(w8))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "least_pressure_status_bits",
        "prev_temp_c",
        "event_seq",
        "last_known_next_pressure",
        "last_known_event_seq",
        "temp_c_cume_dist",
        "row_seq_2",
        F.sum("is_new_session").over(w9).alias("island_id"),
    )
)

with_prev = islands.withColumn("prev_reading_ts", F.lag("reading_ts").over(w8))

gaps_2 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w9))

result = (
    sessionized
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-04"])
    .agg(F.sum("prev_temp_c").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-02_amount").alias("dv_02_amount"),
        F.col("DV-02_cnt").alias("dv_02_cnt"),
        F.col("DV-04_amount").alias("dv_04_amount"),
        F.col("DV-04_cnt").alias("dv_04_cnt"),
    )
)
