from pyspark.sql import functions as F

avg_amount = df_transactions.filter(F.col("account_id").isin([17, 25, 40])).agg(F.avg("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") > avg_amount)
    .select("id", "amount", "txn_type", "merchant")
)
