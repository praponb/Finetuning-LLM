from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("island_id", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount").desc())
w6 = Window.orderBy("island_id")
w7 = Window.partitionBy("status").orderBy(F.col("amount_percent_rank").asc_nulls_last())
w8 = Window.orderBy(F.col("is_new_session").asc_nulls_first())
w9 = Window.partitionBy("status")
w10 = Window.orderBy(F.col("amount_share").desc_nulls_first())
w11 = Window.orderBy("island_id_dense_rank")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-05-27").cast("date"))
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

metrics = (
    sessionized
    .select(
        "id",
        "amount",
        "status",
        "status_lower",
        "method_clean",
        "is_new_session",
        "island_id",
        F.ntile(4).over(w3).alias("island_id_bucket"),
        F.last("method", ignorenulls=True).over(w4).alias("last_known_method"),
    )
)

matched_keys = (
    metrics
    .select("status")
    .intersectAll(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("status")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "amount",
        "status",
        "status_lower",
        "is_new_session",
        F.percent_rank().over(w5).alias("amount_percent_rank"),
        F.dense_rank().over(w6).alias("island_id_dense_rank"),
    )
)

scored = (
    enriched
    .filter(F.col("amount_percent_rank") <= 0.53)
    .select(
        "amount",
        "status_lower",
        "is_new_session",
        "amount_percent_rank",
        "island_id_dense_rank",
        F.percent_rank().over(w7).alias("amount_percent_rank_percent_rank"),
        F.rank().over(w8).alias("is_new_session_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w9)), 4).alias("amount_share"),
        F.stddev_samp("amount_percent_rank").over(w9).alias("status_stddev_samp_amount_percent_rank"),
        F.max("island_id_dense_rank").over(w9).alias("status_max_island_id_dense_rank"),
    )
)

enriched_payments = (
    scored
    .select(
        "status_lower",
        "is_new_session",
        "island_id_dense_rank",
        "amount_share",
        "status_stddev_samp_amount_percent_rank",
        "status_max_island_id_dense_rank",
        F.cume_dist().over(w10).alias("amount_share_cume_dist"),
        F.cume_dist().over(w11).alias("island_id_dense_rank_cume_dist"),
    )
)

daily = (
    enriched_payments
    .groupBy(
        F.when(F.col("island_id_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("island_id_dense_rank_tier"),
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("island_id_dense_rank", 0.95).alias("island_id_dense_rank_p95"),
    )
)

result = (
    daily
    .select(
        "island_id_dense_rank_tier",
        "is_new_session_tier",
        "row_count",
        "island_id_dense_rank_p95",
    )
)
