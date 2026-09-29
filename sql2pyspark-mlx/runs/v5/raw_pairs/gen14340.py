from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w_part = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .select(
        "*",
        F.min("pressure").over(w_part).alias("running_min_pressure"),
        F.sum("pressure").over(w_part).alias("running_sum_pressure"),
        F.count("pressure").over(w_part).alias("running_count_pressure"),
        F.avg("pressure").over(w_part).alias("running_avg_pressure"),
        F.ntile(5).over(w).alias("pressure_bucket"),
    )
)

ranked_device_readings = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_prev = ranked_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "pressure_bucket",
        F.sum("session_start_flag").over(w4).alias("session_id"),
    )
)

enriched_device_readings = (
    islands
    .select(
        "*",
        F.avg("session_id").over(w5).alias("running_avg_session_id"),
        F.count("session_id").over(w5).alias("running_count_session_id"),
        F.max("session_id").over(w5).alias("running_max_session_id"),
        F.sum("session_id").over(w5).alias("running_sum_session_id"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "reading_ts",
        "firmware",
        "pressure_bucket",
        "session_id",
        "running_avg_session_id",
        "running_count_session_id",
        "running_max_session_id",
        "running_sum_session_id",
    )
    .orderBy(F.col("running_avg_session_id").desc())
)
