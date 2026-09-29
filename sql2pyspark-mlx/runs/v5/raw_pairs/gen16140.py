from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(500, 800))
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("max_amount") > 1530)
    .orderBy(F.col("avg_amount").desc())
)
