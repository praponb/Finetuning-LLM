from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        "island_id",
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum("humidity").alias("total_humidity"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("island_id").asc_nulls_first())
)
