from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2023-07-01"), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.status", F.col("c.merchant").alias("transaction_merchant"))
)

base = (
    latest_match
    .filter(F.col("transaction_merchant") != "Starbucks")
    .withColumn("transaction_merchant_status", F.concat("transaction_merchant", F.lit(" - "), "status"))
)

valid = (
    base
    .filter(F.col("status").isNull())
    .withColumn("transaction_merchant_status_2", F.concat("transaction_merchant", F.lit(", "), "status"))
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"), F.col("c.amount"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("amount") >= 4300, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("amount")).otherwise(0)).alias("closed_amount"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .orderBy(F.col("closed_amount").desc())
)
