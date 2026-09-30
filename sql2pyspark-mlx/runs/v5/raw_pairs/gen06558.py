from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("days_since_txn_time").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

filtered = (
    df_transactions
    .filter(F.col("amount").isNotNull() & (F.col("txn_time") > F.to_date(F.lit("2023-06-16"))))
    .select(
        "*",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "days_since_txn_time",
        "resolved_merchant",
    )
)

joined = (
    deduped
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.status"), F.col("d.balance"), F.col("d.account_type"))
)

metrics = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "status",
        "balance",
        "account_type",
        F.lag("amount").over(w2).alias("prev_amount"),
        F.lag("days_since_txn_time").over(w2).alias("prev_days_since_txn_time"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

kept_keys = (
    metrics
    .select("account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
        .select("account_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

filtered_transactions = (
    verified
    .filter(F.col("event_seq").between(3, 4))
    .select(
        "*",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

result = (
    filtered_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "prev_days_since_txn_time",
        "event_seq",
        "is_open",
        "status_filled",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
