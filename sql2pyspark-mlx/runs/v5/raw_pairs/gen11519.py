from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "humidity_filled",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 34.6, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("reading_ts").alias("last_seen"),
        F.percentile_approx("humidity_filled", [0.25, 0.5, 0.75]).alias("humidity_filled_quartiles"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
