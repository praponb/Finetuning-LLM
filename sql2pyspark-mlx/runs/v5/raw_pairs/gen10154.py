from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3800)
    .select("id", "is_flagged", "amount")
)
