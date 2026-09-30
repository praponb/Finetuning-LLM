from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_loan", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("is_loan").desc_nulls_first())
w3 = Window.partitionBy("account_type").orderBy("is_loan", "id")
w4 = Window.partitionBy("account_type").orderBy("prev_balance", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("account_type") == "LOAN", 1).otherwise(0).alias("is_loan"),
    )
)

metrics = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("is_loan_dense_rank", F.dense_rank().over(w2))
    .withColumn("prev_balance", F.lag("balance", 2, 0).over(w3))
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

filtered = (
    latest_match
    .filter(F.col("balance") >= 20000)
    .select(
        "id",
        "opened_date",
        "seq_no",
        "is_loan_dense_rank",
        "transaction_amount",
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

latest_match_accounts = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

daily = (
    latest_match_accounts
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.percentile_approx("seq_no_filled", 0.5).alias("seq_no_filled_p50"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .filter(F.col("num_txn_types") > 1)
)

result = (
    daily
    .select("year_start", "seq_no_filled_p50", "event_count", "num_txn_types")
    .orderBy(F.col("event_count").desc_nulls_last())
)
