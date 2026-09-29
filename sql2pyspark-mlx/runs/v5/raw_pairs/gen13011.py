from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count("*").alias("cnt"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
    .orderBy(F.col("avg_pressure").desc())
)
