from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("k").orderBy("burst_id", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("session_id", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select("id", "created_at", "coupon_codes", F.explode("attributes").alias("k", "v"))
)

gaps = (
    lines
    .withColumn("new_group_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

with_gaps = (
    islands
    .withColumn("is_new_session", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_carts = (
    with_gaps
    .select(
        "id",
        "created_at",
        "k",
        "v",
        "session_id",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

gaps_2 = (
    sessions_carts
    .withColumn("session_start_flag", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands_carts = gaps_2.withColumn("island_id", F.sum("session_start_flag").over(w2))

gaps_3 = (
    islands_carts
    .withColumn("new_group_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_carts_2 = gaps_3.withColumn("session_no", F.sum("new_group_flag").over(w2))

with_gaps_2 = (
    sessions_carts_2
    .withColumn("session_start_flag_2", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("burst_id_2", F.sum("session_start_flag_2").over(w2))

enriched = (
    sessionized
    .filter(F.col("session_no") > 3)
    .select(
        "id",
        "k",
        "v",
        "session_start_flag",
        "island_id",
        "new_group_flag",
        F.sum("new_group_flag").over(w_hist).alias("running_new_group_flag"),
        F.last("session_start_flag_2", ignorenulls=True).over(w_recent).alias("last_known_session_start_flag_2"),
    )
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.min("new_group_flag").alias("min_new_group_flag"),
        F.count("*").alias("cnt"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    totals
    .select(
        "session_start_flag_tier",
        "num_ks",
        "min_new_group_flag",
        "cnt",
        "new_group_flag_quartiles",
    )
    .orderBy(F.col("num_ks").desc())
)
