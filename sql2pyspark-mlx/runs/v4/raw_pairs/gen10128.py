from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("txn_type", "merchant")
w3 = Window.partitionBy("merchant")
w_running = Window.orderBy(F.col("amount_share_2").desc_nulls_first())
w5 = Window.orderBy(F.col("amount").desc())
w6 = Window.partitionBy("account_id").orderBy("amount_share_2")
w_ordered = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_part = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

ranked = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("merchant", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([27, 28]))
        .select("merchant", "account_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("txn_type_min_amount", F.min("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.balance"),
        F.col("d.opened_date"),
        F.col("d.account_type"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.dense_rank().over(w_running).alias("amount_share_2_dense_rank"),
        F.min("amount_share_2").over(w3).alias("merchant_min_amount_share_2"),
        F.percent_rank().over(w5).alias("amount_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.rank().over(w6).alias("amount_share_2_rank"),
        F.sum("amount_share").over(w_ordered).alias("amount_share_sum_90d"),
        F.avg("txn_type_min_amount").over(w_part).alias("txn_type_min_amount_avg_90d"),
    )
)

cleaned = (
    enriched
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-06-22"))) & F.col("account_type").isNotNull())
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "rn",
        "txn_type_min_amount",
        "amount_share",
        "balance",
        "account_type",
        "amount_share_2_dense_rank",
        "amount_percent_rank",
        "amount_share_2_rank",
        "amount_share_sum_90d",
        "txn_type_min_amount_avg_90d",
        F.round("amount_share_2", 0).alias("amount_share_2_rounded"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

result = (
    cleaned
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "DEBIT", "FEE"])
    .agg(
        F.max("amount_share_2_dense_rank").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
