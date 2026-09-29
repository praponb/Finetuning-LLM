from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT"]) & F.col("account_id").isin([8, 18]) & F.col("account_id").isin([7, 17, 25, 35]))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 1500)
    .orderBy("first_txn_time")
)
