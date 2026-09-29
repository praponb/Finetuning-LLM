from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("session_id", "id")

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

with_gaps = (
    sessions_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings_2 = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

deduped = (
    sessions_device_readings_2
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_gaps_2 = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.min("reading_ts").alias("started_at"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
    )
    .orderBy("dv_02_events")
)
