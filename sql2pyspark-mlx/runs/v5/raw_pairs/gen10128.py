from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("day", "reading_ts").alias("activity_day"),
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.max("reading_ts").alias("ended_at"),
    )
    .filter(F.col("dv_01_rows") >= 1)
    .orderBy(F.col("activity_day").desc())
)
