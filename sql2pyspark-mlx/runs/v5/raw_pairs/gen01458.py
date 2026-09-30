from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 34)
    .orderBy("account_id")
)
