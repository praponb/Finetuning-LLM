from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.max("reading_ts").alias("ended_at"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.max("reading_ts").alias("last_seen"),
    )
    .filter(F.col("dv_02_rows") < 1)
    .orderBy(F.col("hour_window").desc_nulls_first())
)
