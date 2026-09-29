from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([37, 39]))
    .select("merchant")
    .distinct()
)
