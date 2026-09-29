from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.hour("reading_ts").between(12, 18))
    .select("*", (F.col("temp_c") + F.col("humidity")).alias("temp_c_plus_humidity"))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("status_bits")).otherwise(0)).alias("dv_04_status_bits"),
        F.count("*").alias("event_count"),
        F.min("reading_ts").alias("started_at"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") >= 0)
)
