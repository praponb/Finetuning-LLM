from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("temp_c") > 38)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        (F.col("status_bits") + F.col("pressure")).alias("status_bits_plus_pressure"),
        F.round((F.col("temp_c") * 100), 2).alias("adjusted_temp_c"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("session_no"))).alias("dv_04_total_session_no"),
    )
    .orderBy("activity_day")
)
