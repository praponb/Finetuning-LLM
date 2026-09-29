from pyspark.sql import functions as F

src = df_transactions.filter(F.col("account_id").isin([15, 38]))

txn_day_summary = (
    src
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = txn_day_summary.orderBy("unique_accounts")
