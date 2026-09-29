from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first(), "id")
w2 = Window.partitionBy("device_code").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("rn").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())

latest = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .withColumn("cumulative_humidity", F.sum("humidity").over(w2))
    .withColumn("pressure_bucket", F.ntile(5).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w5) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_no", F.sum("is_new_burst").over(w6))

with_gaps = (
    sessions_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w5) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no_2", F.sum("session_start_flag").over(w6))

filtered = (
    sessionized
    .filter(F.col("reading_ts") > F.lit("2025-09-01 16:00:00").cast("timestamp"))
    .withColumn("pressure_band", F.when(F.col("pressure") >= 1016, "High").when(F.col("pressure") >= 999, "Medium").otherwise("Low"))
    .withColumn("session_no_2_per_seq_no", F.round((F.col("session_no_2") / F.col("seq_no")), 2))
)

gaps = (
    filtered
    .withColumn("session_start_flag_2", F.when(F.lag("reading_ts").over(w5).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w5) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = gaps.withColumn("session_id", F.sum("session_start_flag_2").over(w6))

ranked = (
    sessionized_device_readings
    .withColumn("rn_2", F.rank().over(w7))
    .filter(F.col("rn_2") == 1)
)

wide = (
    ranked
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03"])
    .agg(F.max("session_no_2_per_seq_no"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-03").alias("dv_03"))
)

result = (
    wide
    .select("firmware", "dv_01", "dv_03")
    .orderBy(F.col("dv_03").desc())
)
