from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-5, -1)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("status_bits").between(4, 6))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.when(F.col("temp_c") >= 37, "High").when(F.col("temp_c") >= 23.7, "Medium").otherwise("Low").alias("temp_c_level"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

metrics = (
    islands
    .filter(F.col("burst_id") > 8)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "temp_c_level",
        "session_start_flag",
        F.corr("pressure", "humidity").over(w3).alias("corr_pressure_humidity"),
        F.avg("burst_id").over(w4).alias("burst_id_avg_5_prior"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "temp_c_level",
        "session_start_flag",
        "corr_pressure_humidity",
        "burst_id_avg_5_prior",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

latest = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_gaps_2 = (
    latest
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized_device_readings = (
    with_gaps_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "temp_c_level",
        "session_start_flag",
        "corr_pressure_humidity",
        "burst_id_avg_5_prior",
        "session_no",
        "rank_in_group",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

stats = (
    sessionized_device_readings
    .groupBy("burst_id", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
    )
)

result = stats.select("burst_id", "hour_window")
