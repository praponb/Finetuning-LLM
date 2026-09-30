from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host", "service").orderBy(F.col("session_id").desc(), "id", "attr_key")
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

with_gaps = (
    exploded
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = (
    with_gaps
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "attr_key",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

scored = (
    sessions_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "attr_key",
        "session_id",
        F.ntile(4).over(w3).alias("session_id_bucket"),
        F.max("session_id").over(w4).alias("session_id_max_1h"),
    )
)

with_gaps_2 = (
    scored
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_app_logs_2 = with_gaps_2.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

valid = (
    sessions_app_logs_2
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & (F.col("level") == "DEBUG"))
    .select(
        "id",
        "log_ts",
        "attr_key",
        "session_id_bucket",
        "session_id_max_1h",
        "new_group_flag",
        "session_id_2",
        F.date_trunc("year", "log_ts").alias("log_ts_period"),
        F.round((F.col("session_id_2") / F.col("session_id_bucket")), 2).alias("session_id_2_per_session_id_bucket"),
    )
)

result = (
    valid
    .groupBy("attr_key")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count").desc())
)
