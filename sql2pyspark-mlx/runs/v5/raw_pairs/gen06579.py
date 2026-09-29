from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Starbucks")
    .select("merchant")
    .distinct()
    .orderBy("merchant")
)
