from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") < 3)
    .drop("amount_count")
    .orderBy(F.col("min_amount").desc())
)
