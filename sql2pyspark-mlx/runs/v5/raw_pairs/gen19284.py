from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w3 = Window.partitionBy("account_type", "account_id").orderBy(F.col("txn_time").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-04 06:00:00").cast("timestamp")) & F.col("amount").between(2500, 3300))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged_flag",
        F.col("p2.balance"),
        F.col("p2.account_type"),
        F.col("p2.opened_date"),
    )
)

enriched = (
    joined
    .filter(F.col("is_flagged_flag").between(28, 78))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "account_type",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.lead("is_flagged_flag", 1, 0).over(w2).alias("next_is_flagged_flag"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "account_type",
        "amount_share",
        F.col("sum_transaction_amount"),
    )
)
