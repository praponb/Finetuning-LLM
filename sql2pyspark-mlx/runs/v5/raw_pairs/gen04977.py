from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("firmware") == "1.1.0"))
    .select("*", F.round((F.col("status_bits") * 1.15), 2).alias("status_bits_scaled"))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

filtered_device_readings = (
    sessions_device_readings
    .filter(F.col("humidity").between(46, 50))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_scaled",
        "is_new_burst",
        "burst_id",
        (F.col("burst_id") * F.col("is_new_burst")).alias("burst_id_times_is_new_burst"),
        F.round((F.col("temp_c") * 1.1), 2).alias("temp_c_adjusted"),
    )
)

cleaned = (
    filtered_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 08:00:00")))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "is_new_burst",
        "burst_id",
        "burst_id_times_is_new_burst",
        (F.col("pressure") / F.nullif("burst_id_times_is_new_burst", F.lit(0))).alias("pressure_per_burst_id_times_is_new_burst"),
    )
)

result = (
    cleaned
    .groupBy("burst_id")
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.corr("humidity", "status_bits").alias("corr_humidity_status_bits"),
        F.min("reading_ts").alias("first_seen"),
        F.bit_and("status_bits").alias("status_bits_bit_and_2"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
    )
)
