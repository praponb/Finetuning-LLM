from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.max(F.when(F.col("device_code") == "DV-01", F.col("session_no"))).alias("dv_01_max_session_no"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("is_new_session"))).alias("dv_03_avg_is_new_session"),
    )
)
