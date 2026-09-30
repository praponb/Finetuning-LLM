from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("humidity") < 75)
    .withColumn("adjusted_pressure", F.round((F.col("pressure") * 0.9), 2))
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .rollup(
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("status_bits") >= 12, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.max("pressure").alias("max_pressure"),
        F.percentile_approx("is_new_session", 0.9).alias("is_new_session_p90"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.min("reading_ts").alias("started_at"),
        F.round(F.sum("adjusted_pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("adjusted_pressure_per_dv_03"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
