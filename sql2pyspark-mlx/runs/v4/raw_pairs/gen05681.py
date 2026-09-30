from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1900)
    .select("id", "merchant")
    .unionByName(df_transactions.filter(F.col("is_flagged")).select("id", "merchant"))
)
