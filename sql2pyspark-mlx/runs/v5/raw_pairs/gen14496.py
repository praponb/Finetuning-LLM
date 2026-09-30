from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

result = (
    latest_match
    .groupBy("status", F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("balance_per_savings"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.min("transaction_txn_time").alias("first_seen"),
    )
    .filter(F.col("balance_per_savings") < 1000000)
    .orderBy("credit_rows")
)
