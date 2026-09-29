from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
    )
    .orderBy("merchant")
)
