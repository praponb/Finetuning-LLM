from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "CREDIT"]) & (F.col("amount") <= 3500))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"), "account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("min_amount") > 3700)
    .orderBy("account_id")
)
