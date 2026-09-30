from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("burst_id")
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.min("reading_ts").alias("started_at"),
        F.avg(F.when(F.col("device_code") == "DV-02", F.col("temp_c"))).alias("dv_02_avg_temp_c"),
    )
    .filter(F.col("dv_02_avg_temp_c") < 1000000)
    .orderBy("dv_02_avg_temp_c")
)
