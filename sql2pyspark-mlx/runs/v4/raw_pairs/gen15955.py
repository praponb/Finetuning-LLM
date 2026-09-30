from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc())
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.partitionBy("device_code").orderBy("session_id")
w6 = Window.partitionBy("device_code")

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized_device_readings = gaps.withColumn("burst_id_2", F.sum("is_new_session").over(w2))

latest = (
    sessionized_device_readings
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

gaps_2 = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("prev_burst_id_2", F.lag("burst_id_2", 1, 0).over(w4))
    .withColumn("session_id_percent_rank", F.percent_rank().over(w5))
    .withColumn("corr_temp_c_pressure", F.corr("temp_c", "pressure").over(w6))
)
