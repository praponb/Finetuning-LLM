from pyspark.sql import functions as F

avg_amount = df_transactions.filter(F.col("txn_time") < "2023-08-31").agg(F.avg("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") > avg_amount)
    .select("id", "amount")
)
