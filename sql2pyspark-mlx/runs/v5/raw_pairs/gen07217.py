from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("merchant").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first())
w5 = Window.orderBy(F.col("balance_cume_dist").desc(), "id")
w_running = Window.orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy(F.col("seq_no").asc_nulls_first())
w_recent = Window.partitionBy("merchant").orderBy("amount", "id")

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-05-18 11:00:00").cast("timestamp"))
    .withColumn("txn_time_label", F.date_format("txn_time", "MMM yyyy"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("OTHER")))
)

enriched = (
    filtered
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.merchant",
        F.col("b.opened_date"),
        F.col("b.balance"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "balance",
        F.max("amount").over(w).alias("account_id_max_amount"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
    )
)

scored = (
    metrics
    .filter(F.col("merchant").isin(["Starbucks", "Grab", "Amazon"]))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_dense_rank", F.dense_rank().over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

scored_transactions = (
    scored
    .select(
        "id",
        "merchant",
        "balance",
        "account_id_max_amount",
        "balance_cume_dist",
        "amount_dense_rank",
        "row_seq",
        F.avg("account_id_max_amount").over(w_running).alias("account_id_max_amount_avg_3"),
        F.dense_rank().over(w7).alias("seq_no_dense_rank"),
        F.ntile(3).over(w_recent).alias("amount_bucket"),
    )
)

result = (
    scored_transactions
    .select(
        "id",
        "amount_dense_rank",
        "row_seq",
        "account_id_max_amount_avg_3",
        "seq_no_dense_rank",
        "amount_bucket",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
