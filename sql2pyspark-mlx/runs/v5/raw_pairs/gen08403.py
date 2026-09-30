from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
    .orderBy(F.col("max_amount").desc(), "method", "status")
    .limit(3)
)
