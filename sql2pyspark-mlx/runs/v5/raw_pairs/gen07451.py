from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.date_trunc("month", "txn_time").alias("txn_time_period"))
    .orderBy("amount")
)
