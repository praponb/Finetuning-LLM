from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure_stddev").desc())

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("device_code", "session_id")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
    )
    .filter(F.col("dv_03_count") < 1000)
    .withColumn("pressure_stddev_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("avg_pressure").desc())
)
