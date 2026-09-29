from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("humidity_per_dv_04"),
        F.count("*").alias("row_count"),
        F.max("temp_c").alias("max_temp_c"),
    )
    .filter(F.col("max_temp_c") >= 0)
    .orderBy(F.col("hour_window").asc_nulls_first())
)
