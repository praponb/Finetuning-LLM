from pyspark.sql import functions as F

min_amount = df_transactions.filter(F.col("txn_time") >= "2025-03-24").agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") <= min_amount)
    .select("id", "amount")
    .orderBy("id")
    .limit(5)
)
