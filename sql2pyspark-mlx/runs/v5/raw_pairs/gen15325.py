from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k")
w4 = Window.partitionBy("service").orderBy(F.col("session_id").desc(), "id", "k")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("session_id_max_2h")
w7 = Window.partitionBy("k").orderBy("session_id_bucket", "id", "k").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("service").orderBy("session_id", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

prev_events = exploded.withColumn("prev_log_ts", F.lag("log_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "attr_value",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

scored = (
    sessionized
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "attr_value",
        "session_id",
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.sum("session_id").over(w3).alias("k_sum_session_id"),
        F.ntile(4).over(w4).alias("session_id_bucket"),
        F.max("session_id").over(w5).alias("session_id_max_2h"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "k",
        "attr_value",
        "first_known_k",
        "session_id_max_2h",
        "latest_rank",
        F.max("session_id_bucket").over(w7).alias("last7_max_session_id_bucket"),
        F.sum("session_id_bucket").over(w7).alias("last7_sum_session_id_bucket"),
        F.min("session_id_bucket").over(w7).alias("last7_min_session_id_bucket"),
        F.avg("session_id_bucket").over(w7).alias("last7_avg_session_id_bucket"),
        F.first("session_id_max_2h", ignorenulls=True).over(w8).alias("first_known_session_id_max_2h"),
    )
)
