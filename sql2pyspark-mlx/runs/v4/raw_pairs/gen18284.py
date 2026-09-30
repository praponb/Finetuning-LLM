from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.count("*").alias("num_transactions"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("num_transactions").desc())
)
