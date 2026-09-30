from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1600, 3700))
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
