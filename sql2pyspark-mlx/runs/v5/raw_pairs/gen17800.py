from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("merchant", "account_id")
w6 = Window.partitionBy("txn_type")
w7 = Window.partitionBy("account_id").orderBy(F.col("event_seq").desc(), "id")
w8 = Window.partitionBy("account_status").orderBy("txn_time", "id")
w9 = Window.partitionBy("account_status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

valid = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Amazon"))) & (F.col("txn_time") > F.lit("2023-07-18 00:00:00").cast("timestamp")))
    .select("*", F.lower("merchant").alias("merchant_lower"))
)

enriched = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("amount_max_7d", F.max("amount").over(w2))
    .withColumn("amount_sum_30d", F.sum("amount").over(w3))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w4))
)

scored = (
    enriched
    .withColumn("merchant_sum_event_seq", F.sum("event_seq").over(w5))
    .withColumn("amount_max_7d_share", F.round((F.col("amount_max_7d") / F.sum("amount_max_7d").over(w6)), 4))
    .withColumn("event_seq_bucket", F.ntile(10).over(w7))
)

enriched_transactions = (
    scored
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.balance") < 66000)
    .select("src.*", F.col("d.status"), F.col("d.account_type"))
)

joined = (
    enriched_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.merchant_lower",
        "a.event_seq",
        "a.amount_max_7d",
        "a.amount_avg_6_prior",
        "a.amount_max_7d_share",
        F.col("b.status").alias("account_status"),
        F.col("b.opened_date"),
    )
)

with_gaps = (
    joined
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w8).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w8) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w9))

result = (
    islands
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "merchant_lower",
        "event_seq",
        "amount_max_7d",
        "amount_avg_6_prior",
        "amount_max_7d_share",
        "opened_date",
        F.col("max_transaction_amount"),
    )
)
