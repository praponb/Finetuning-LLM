from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts_period", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("status_bits") < 10))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        F.date_trunc("year", "reading_ts").alias("reading_ts_period"),
    )
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("reading_ts_period").over(w).isNull() | (F.col("reading_ts_period") > F.lag("reading_ts_period").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("is_new_burst")).otherwise(0)).alias("dv_01_is_new_burst"),
    )
    .filter(F.col("dv_01_is_new_burst") > 0)
    .orderBy("device_code")
)
