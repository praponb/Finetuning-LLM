from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("log_ts")
w4 = Window.partitionBy("k").orderBy("new_group_flag", "id", "k")
w5 = Window.partitionBy("level", "k").orderBy("rank_in_group")
w6 = Window.partitionBy("k").orderBy("new_group_flag", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level", "k").orderBy("rank_in_group", "id", "k").rowsBetween(-3, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

gaps = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

ranked = (
    islands
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands_app_logs = (
    flagged
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        "new_group_flag",
        "island_id",
        "rank_in_group",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

ranked_app_logs = (
    islands_app_logs
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked_app_logs
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "log_ts",
        "k",
        "new_group_flag",
        "island_id",
        "rank_in_group",
        "rn",
        F.first("k", ignorenulls=True).over(w6).alias("first_known_k"),
        F.avg("session_no").over(w7).alias("session_no_avg_3_prior"),
    )
)
