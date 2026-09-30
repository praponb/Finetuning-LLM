from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy(F.col("amount_dense_rank").desc_nulls_last())
w_part = Window.partitionBy("method").orderBy(F.col("amount_dense_rank").desc())
w8 = Window.orderBy(F.col("amount_count_7d").asc_nulls_first())
w9 = Window.partitionBy("method")

cleaned = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("bank_transfer"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.initcap(F.trim("method")).alias("method_title"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

with_prev = cleaned.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "method_clean",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

remaining_keys = (
    islands
    .select("method")
    .exceptAll(
        df_payments
        .filter(F.col("amount").between(420, 1330))
        .select("method")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-09-29"))) & F.col("method").isNotNull())
    .select(
        "method",
        "amount",
        "paid_at",
        F.lower("method").alias("method_lower"),
        F.when(F.col("method").isin(["cash", "paypal"]), "Group A").otherwise("Other").alias("method_group"),
    )
)

with_windows = (
    valid
    .select(
        "method",
        "paid_at",
        "method_lower",
        "method_group",
        F.dense_rank().over(w_hist).alias("amount_dense_rank"),
        F.max("amount").over(w4).alias("amount_max_90d"),
        F.count("amount").over(w_recent).alias("amount_count_7d"),
    )
)

metrics = (
    with_windows
    .withColumn("amount_dense_rank_rank", F.rank().over(w6))
    .withColumn("amount_dense_rank_cume_dist", F.cume_dist().over(w_part))
    .withColumn("amount_count_7d_rank", F.rank().over(w8))
    .withColumn("amount_max_90d_share", F.round((F.col("amount_max_90d") / F.sum("amount_max_90d").over(w9)), 4))
)

base = (
    metrics
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.weekofyear("paid_at").alias("paid_at_weekofyear"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

result = (
    base
    .select(
        "paid_at",
        "method_lower",
        "amount_dense_rank",
        "amount_dense_rank_rank",
        "amount_dense_rank_cume_dist",
        "amount_count_7d_rank",
        "amount_max_90d_share",
        "paid_at_weekofyear",
        "method_clean",
    )
    .orderBy("paid_at_weekofyear")
)
