from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 08:00:00"))) & (F.col("device_code") != "DV-04"))
    .withColumn("device_code_lower", F.lower("device_code"))
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("humidity_per_dv_02"),
        F.min("prev_reading_ts").alias("first_prev_reading_ts"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("island_id")).otherwise(0)).alias("dv_04_island_id"),
    )
)
