from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        "device_code",
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.min("reading_ts").alias("started_at"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.avg("burst_id").alias("avg_burst_id"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.min("reading_ts").alias("started_at_2"),
    )
    .filter(F.col("dv_02_rows") > 1)
    .orderBy(F.col("avg_burst_id").desc())
)
