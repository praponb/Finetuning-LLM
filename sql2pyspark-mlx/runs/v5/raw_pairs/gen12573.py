from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flagged = (
    flattened
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "v",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

with_gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w3) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

cleaned = (
    sessions_app_logs
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select("*", F.round("burst_id", 0).alias("burst_id_rounded"))
)

matched_keys = (
    cleaned
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.col("status_code").between(346, 416))
        .select("id", "service")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "service", "k", "v", "is_new_burst", "burst_id", "burst_id_rounded")
    .orderBy(F.col("is_new_burst").desc_nulls_last())
)
