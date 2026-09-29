from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 28, 31]))
    .select("id", "account_id", "amount")
)
