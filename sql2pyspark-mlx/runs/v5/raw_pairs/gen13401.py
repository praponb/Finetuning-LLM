from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first(), "id")
w_grp = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last())
w8 = Window.orderBy("pressure", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("device_code") != "DV-01")
    .withColumn("reading_ts_period", F.date_trunc("WEEK", "reading_ts"))
    .withColumn("firmware_device_code", F.concat("firmware", F.lit(" - "), "device_code"))
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_device_readings = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped_device_readings
    .withColumn("last7_avg_pressure", F.avg("pressure").over(w_grp))
    .withColumn("last7_max_pressure", F.max("pressure").over(w_grp))
    .withColumn("last7_min_pressure", F.min("pressure").over(w_grp))
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w4))
)

prev_events = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "reading_ts_period",
        "firmware_device_code",
        "row_num",
        "dedup_rank",
        "last7_max_pressure",
        "last7_min_pressure",
        F.sum("session_start_flag").over(w5).alias("island_id"),
    )
)

enriched = (
    sessions_device_readings
    .select(
        "*",
        F.last("last7_max_pressure", ignorenulls=True).over(w6).alias("last_known_last7_max_pressure"),
        F.percent_rank().over(w7).alias("pressure_percent_rank"),
        F.avg("last7_max_pressure").over(w8).alias("last7_max_pressure_avg_5"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "temp_c",
        "reading_ts_period",
        "firmware_device_code",
        "row_num",
        "dedup_rank",
        "last7_max_pressure",
        "island_id",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "CORE").when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("temp_c").desc())
)
