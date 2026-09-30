from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("method").orderBy(F.col("amount_dense_rank").asc_nulls_first())
w_ordered = Window.partitionBy("method").orderBy(F.col("amount_dense_rank").desc_nulls_first())

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-07 14:00:00").cast("timestamp")) & F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

kept_keys = (
    filtered
    .select("id", "order_id")
    .intersectAll(
        df_payments
        .filter(F.col("amount") >= 1220)
        .select("id", "order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("method") == "paypal")
    .select("*", F.dense_rank().over(w_part).alias("amount_dense_rank"))
)

base = (
    with_windows
    .filter((F.col("paid_at") > F.lit("2023-10-16 05:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status_prefix",
        "amount_dense_rank",
        F.coalesce("amount_dense_rank", F.lit(0)).alias("amount_dense_rank_filled"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

scored = (
    base
    .withColumn("amount_dense_rank_dense_rank", F.dense_rank().over(w2))
    .withColumn("amount_dense_rank_dense_rank_2", F.dense_rank().over(w_ordered))
)

filtered_payments = (
    scored
    .filter(F.col("method").isNotNull() & (F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select("*", F.substring("method", 1, 2).alias("method_prefix"))
)

stats = (
    filtered_payments
    .groupBy(
        F.when(F.col("amount_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("amount_dense_rank_tier"),
    )
    .agg(
        F.round(F.sum("amount_dense_rank_filled") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_dense_rank_filled_per_paypal"),
        F.percentile_approx("amount_dense_rank_dense_rank", [0.25, 0.5, 0.75]).alias("amount_dense_rank_dense_rank_quartiles"),
        F.countDistinct("method").alias("distinct_methods"),
    )
)

result = (
    stats
    .select("amount_dense_rank_tier")
    .orderBy(F.col("amount_dense_rank_tier").desc_nulls_last())
    .limit(20)
)
