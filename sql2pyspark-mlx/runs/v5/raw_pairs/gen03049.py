from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 1100)
    .select("is_flagged", "avg_amount", "min_amount")
)
