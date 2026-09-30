from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("pressure_adjusted", F.round((F.col("pressure") * 0.85), 2))
    .withColumn("status_bits_whole", F.ceil("status_bits"))
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("status_bits_whole") >= 65, "LARGE").otherwise("SMALL").alias("status_bits_whole_tier"),
        F.when(F.col("pressure_adjusted") >= 83, "LARGE").otherwise("SMALL").alias("pressure_adjusted_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("humidity"))).alias("dv_03_total_humidity"),
        F.max("reading_ts").alias("last_seen"),
        F.min("reading_ts").alias("started_at"),
    )
    .filter(F.col("dv_03_total_humidity") > 0)
    .orderBy(F.col("activity_day").desc_nulls_first())
)
