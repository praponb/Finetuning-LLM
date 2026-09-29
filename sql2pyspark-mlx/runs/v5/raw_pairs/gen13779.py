from pyspark.sql import functions as F

max_amount = df_transactions.agg(F.max("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") >= max_amount)
    .select("id", "amount", "is_flagged", "account_id")
    .orderBy(F.col("id").desc(), "account_id")
)
