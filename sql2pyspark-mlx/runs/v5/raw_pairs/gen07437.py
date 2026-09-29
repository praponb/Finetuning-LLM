from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc_nulls_last(), "id")
w_part = Window.partitionBy("merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("merchant")
w5 = Window.partitionBy("merchant").orderBy("event_seq")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .filter(F.col("rank_in_group") >= 1)
    .select(
        "*",
        F.lead("amount").over(w_part).alias("next_amount"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

remaining_keys = (
    scored
    .select("merchant", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("merchant") == "Amazon")
        .select("merchant", "account_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "rank_in_group",
        "next_amount",
        "event_seq",
        F.sum("next_amount").over(w3).alias("next_amount_sum_30d"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4).alias("event_seq_share"),
        F.rank().over(w5).alias("event_seq_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

base = (
    enriched
    .filter(F.col("event_seq").between(4, 5))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "next_amount",
        "event_seq",
        "next_amount_sum_30d",
        F.col("rank_in_group").cast("double").alias("rank_in_group_str"),
    )
)

daily = (
    base
    .groupBy("account_id", "txn_type")
    .agg(
        F.max("amount").alias("max_amount"),
        F.max("next_amount").alias("max_next_amount"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
    )
)

result = (
    daily
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_id",
        "txn_type",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "CORE").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "PRIORITY").otherwise("EXTENDED").alias("txn_type_zone"),
        F.col("avg_transaction_amount"),
    )
)
