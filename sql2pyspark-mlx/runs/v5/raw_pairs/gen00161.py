from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("session_id", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("device_code_length", F.length("device_code"))
    .withColumn("status_bits_times_humidity", (F.col("status_bits") * F.col("humidity")))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "device_code_length",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

filtered = (
    sessions_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 07:00:00"))) & ~F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "device_code_length",
        "session_id",
        F.round((F.col("pressure") / F.col("temp_c")), 2).alias("pressure_per_temp_c"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "pressure",
        "device_code_length",
        "session_id",
        "pressure_per_temp_c",
        F.avg("device_code_length").over(w_grp).alias("device_code_length_avg_6"),
        F.count("pressure").over(w4).alias("pressure_count_1h"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("pressure_count_1h") >= 14, "LARGE").otherwise("SMALL").alias("pressure_count_1h_tier"),
    )
    .agg(
        F.max("pressure").alias("max_pressure"),
        F.count("*").alias("row_count"),
    )
)

result = daily.select("pressure_count_1h_tier", "max_pressure", "row_count")
