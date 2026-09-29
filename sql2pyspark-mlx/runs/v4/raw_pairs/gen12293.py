from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

enriched = (
    df_device_readings
    .withColumn("last_known_temp_c", F.last("temp_c", ignorenulls=True).over(w))
    .withColumn("corr_humidity_pressure", F.corr("humidity", "pressure").over(w2))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

flagged = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("session_start_flag").over(w4))

latest = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

flagged_2 = (
    latest
    .withColumn("session_start_flag_2", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged_2.withColumn("session_no", F.sum("session_start_flag_2").over(w4))

result = (
    sessionized
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.max("reading_ts").alias("last_seen"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy("hour_window")
)
