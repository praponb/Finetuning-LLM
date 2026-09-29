from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("status").orderBy(F.col("amount_percent_rank").asc_nulls_first())
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy(F.col("next_amount").desc())
w6 = Window.partitionBy("method")
w7 = Window.partitionBy("method", "status")
w8 = Window.partitionBy("method", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-12-15 06:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_payments
        .filter(F.hour("paid_at").between(6, 10))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.rank().over(w3).alias("amount_percent_rank_rank"),
        F.round((F.col("next_amount") / F.sum("next_amount").over(w4)), 4).alias("next_amount_share"),
        F.rank().over(w5).alias("next_amount_rank"),
        F.sum("next_amount").over(w6).alias("method_sum_next_amount"),
    )
)

with_windows = (
    scored
    .withColumn("method_sum_next_amount_share", F.round((F.col("method_sum_next_amount") / F.sum("method_sum_next_amount").over(w7)), 4))
    .withColumn("amount_percent_rank_rank_max_90d", F.max("amount_percent_rank_rank").over(w8))
)

base = (
    with_windows
    .filter(F.col("method").isNotNull())
    .select(
        "method",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "amount_percent_rank_rank",
        "next_amount_share",
        "method_sum_next_amount",
        "method_sum_next_amount_share",
        F.weekofyear("paid_at").alias("paid_at_weekofyear"),
    )
)

totals = (
    base
    .cube("status")
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.round(F.avg("next_amount_share"), 2).alias("avg_next_amount_share"),
        F.percentile_approx("method_sum_next_amount_share", [0.25, 0.5, 0.75]).alias("method_sum_next_amount_share_quartiles"),
        F.countDistinct("method").alias("distinct_methods"),
    )
)

result = (
    totals
    .select(
        "failed_events",
        "avg_next_amount_share",
        "method_sum_next_amount_share_quartiles",
        "distinct_methods",
    )
    .orderBy(F.col("distinct_methods").desc_nulls_last(), F.col("failed_events").desc())
)
