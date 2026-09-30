from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type", "status").orderBy("last_known_balance", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("last_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

enriched = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.last("balance", ignorenulls=True).over(w3).alias("last_known_balance"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "account_type",
        "seq_no",
        "balance_dense_rank",
        "last_known_balance",
        F.avg("last_known_balance").over(w4).alias("last_known_balance_avg_3"),
        F.sum("balance").over(w5).alias("running_balance"),
        F.avg("balance").over(w6).alias("balance_avg_3"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.seq_no",
        "s.last_known_balance_avg_3",
        "s.balance_avg_3",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("transaction_txn_type").eqNullSafe(F.lit("CREDIT")))
    .withColumn("balance_avg_3_per_last_known_balance_avg_3", (F.col("balance_avg_3") / F.nullif("last_known_balance_avg_3", F.lit(0))))
    .withColumn("account_type_transaction_txn_type_key", F.concat_ws("-", "account_type", "transaction_txn_type"))
    .withColumn("transaction_txn_type_clean", F.coalesce("transaction_txn_type", F.lit("OTHER")))
)

kept = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id") == 38), "left_semi")
)

pivoted = (
    kept
    .groupBy("transaction_txn_type")
    .pivot("account_type", ["CHECKING", "CREDIT"])
    .agg(F.sum("balance_avg_3").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    pivoted
    .select(
        "transaction_txn_type",
        "CHECKING_amount",
        "CHECKING_cnt",
        "CREDIT_amount",
        F.when(F.col("transaction_txn_type").rlike("^(FEE|DEBIT)$"), "GROUP_A").otherwise("CORE").alias("transaction_txn_type_zone"),
    )
    .orderBy(F.col("transaction_txn_type").desc())
    .limit(20)
)
