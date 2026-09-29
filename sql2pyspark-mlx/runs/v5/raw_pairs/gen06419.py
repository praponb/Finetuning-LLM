from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Amazon", "Lazada"]))
    .groupBy("is_flagged")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 3200)
    .orderBy("max_amount")
)
