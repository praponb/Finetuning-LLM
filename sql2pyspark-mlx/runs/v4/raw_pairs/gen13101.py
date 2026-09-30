from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 1200)
    .select("merchant")
    .unionByName(df_transactions.filter(F.col("amount") < 2600).select("merchant"))
    .distinct()
    .orderBy("merchant")
)
