from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("balance") >= 79000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("transaction_txn_time")) - F.unix_timestamp(F.min("transaction_txn_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("transaction_merchant")).alias("transaction_merchant_list"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)
