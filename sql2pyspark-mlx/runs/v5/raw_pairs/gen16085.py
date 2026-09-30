from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("account_balance").asc_nulls_first(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-06-05").cast("date"))
    .withColumn("merchant_length", F.length("merchant"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") == "LOAN")
    .select("e.*", F.col("a.balance"), F.col("a.status"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_time",
        "a.merchant_length",
        F.col("d.balance").alias("account_balance"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.merchant_length")), 2).alias("total_merchant_length"),
        F.grouping_id().alias("level_id"),
    )
)
