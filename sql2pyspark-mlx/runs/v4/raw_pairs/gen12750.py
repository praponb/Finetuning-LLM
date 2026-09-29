from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("island_id", F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.corr("temp_c", "pressure").alias("corr_temp_c_pressure"),
        F.min("reading_ts").alias("started_at"),
        F.avg("humidity").alias("avg_humidity"),
    )
    .orderBy(F.col("avg_humidity").desc_nulls_first())
)
