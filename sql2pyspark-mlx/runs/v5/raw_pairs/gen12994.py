from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("txn_type_prefix", F.substring("txn_type", 1, 4))
)

base = (
    cleaned
    .filter((F.col("txn_time") >= F.lit("2023-08-12 16:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_filled",
        "txn_type_prefix",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "amount_filled",
        "txn_type_prefix",
        "is_flagged_label",
        F.sum("new_group_flag").over(w3).alias("island_id"),
    )
)

enriched = (
    sessions_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.status"))
)

result = (
    enriched
    .groupBy("txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount_filled").alias("max_amount_filled"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy(F.col("max_amount_filled").desc_nulls_first())
)
