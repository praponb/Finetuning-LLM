from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure_rounded").desc_nulls_last(), "id")
w5 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id_tier").orderBy(F.col("session_id_p95").desc())

base = (
    df_device_readings
    .filter(~F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "*",
        F.round("pressure", 0).alias("pressure_rounded"),
        F.upper("device_code").alias("device_code_upper"),
    )
)

valid = (
    base
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select("*", F.initcap(F.trim("firmware")).alias("firmware_title"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

prev_events = latest.withColumn("prev_reading_ts", F.lag("reading_ts").over(w2))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

with_gaps = (
    sessions_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings_2 = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

latest_device_readings = (
    sessions_device_readings_2
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

flagged = (
    latest_device_readings
    .withColumn("is_new_burst_2", F.when(F.lag("prev_reading_ts").over(w5).isNull() | (F.col("prev_reading_ts") > F.lag("prev_reading_ts").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("is_new_burst_2").over(w6))

result = (
    islands
    .groupBy(
        F.when(F.col("session_id") >= 9, "LARGE").otherwise("SMALL").alias("session_id_tier"),
        F.when(F.col("humidity") >= 80, "HIGH").otherwise("LOW").alias("humidity_tier"),
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
        F.max("session_no").alias("max_session_no"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("status_bits_per_dv_03"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("pressure_per_dv_01"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
    )
    .withColumn("session_id_p95_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("humidity_tier").desc_nulls_first())
)
