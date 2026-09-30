from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
