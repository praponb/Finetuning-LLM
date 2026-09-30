from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("greatest_temp_c_humidity_filled", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("temp_c") > 32)
    .withColumn("status_bits_str", F.col("status_bits").cast("double"))
)

base = (
    filtered_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits_str",
        F.greatest("temp_c", "humidity_filled").alias("greatest_temp_c_humidity_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    latest
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "greatest_temp_c_humidity_filled",
        F.sum("is_new_session").over(w3).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
        F.when(F.col("greatest_temp_c_humidity_filled") >= 79, "LARGE").otherwise("SMALL").alias("greatest_temp_c_humidity_filled_tier"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
    .filter(F.col("dv_04_events") >= 1)
    .orderBy(F.col("dv_04_events").asc_nulls_last())
)
