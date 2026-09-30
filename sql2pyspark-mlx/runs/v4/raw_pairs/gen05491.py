from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
    )
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .rollup(
        F.when(F.col("session_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        "device_code",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("is_new_session")).otherwise(0)).alias("dv_03_is_new_session"),
        F.max("reading_ts").alias("ended_at"),
        F.min("burst_id").alias("min_burst_id"),
    )
    .orderBy(F.col("min_burst_id").asc_nulls_last())
)
