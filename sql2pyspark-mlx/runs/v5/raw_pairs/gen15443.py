from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

prev_events = islands.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

remaining_keys = (
    sessions_device_readings
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") < 72)
        .select("id")
    )
)

kept_rows = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("humidity") < 50)
    .select(
        "id",
        "reading_ts",
        "firmware",
        "session_no",
        F.sum("session_no").over(w3).alias("device_code_sum_session_no"),
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share"),
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share_2"),
        F.min("session_no").over(w3).alias("device_code_min_session_no"),
    )
)

result = (
    with_windows
    .withColumn("device_code_min_session_no_tier", F.when(F.col("device_code_min_session_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .cube("device_code_min_session_no_tier")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("device_code_sum_session_no", 0.5).alias("device_code_sum_session_no_p50"),
        F.grouping("device_code_min_session_no_tier").alias("grouping_level"),
    )
    .orderBy("device_code_min_session_no_tier")
)
