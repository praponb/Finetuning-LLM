from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .select("id", "is_flagged")
)
