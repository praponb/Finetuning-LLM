from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host", "k").orderBy(F.col("latency_ms_share_2").desc(), "id", "k")
w7 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w8 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.row_number().over(w2).alias("row_seq"),
        F.min("latency_ms").over(w).alias("k_min_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w4) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w5))

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "latency_ms_share",
        "row_seq",
        "k_min_latency_ms",
        "latency_ms_share_2",
        "is_new_burst",
        "island_id",
        "row_num",
        F.when(F.col("island_id") >= 6, "High").when(F.col("island_id") >= 1, "Medium").otherwise("Low").alias("island_id_level"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w7).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w7) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized_app_logs = flagged.withColumn("session_no", F.sum("new_group_flag").over(w8))

valid = (
    sessionized_app_logs
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "row_seq",
        "k_min_latency_ms",
        "is_new_burst",
        "row_num",
        "island_id_level",
        "k_clean",
        "new_group_flag",
        "session_no",
        F.greatest("row_num", "latency_ms").alias("greatest_row_num_latency_ms"),
        (F.col("k_min_latency_ms") + F.col("island_id")).alias("k_min_latency_ms_plus_island_id"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

result = (
    valid
    .select(
        "id",
        "latency_ms",
        "k",
        "row_seq",
        "is_new_burst",
        "row_num",
        "k_clean",
        "new_group_flag",
        "session_no",
    )
    .orderBy(F.col("row_num").desc_nulls_last())
)
