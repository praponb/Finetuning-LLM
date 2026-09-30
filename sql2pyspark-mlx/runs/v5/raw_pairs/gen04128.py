from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1000)
    .select("merchant")
    .unionByName(df_transactions.filter(F.col("merchant") != "Grab").select("merchant"))
    .distinct()
    .orderBy("merchant")
)
