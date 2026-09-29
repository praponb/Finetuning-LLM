from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "account_id")
    .orderBy(F.col("account_id").desc())
)
