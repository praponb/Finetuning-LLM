from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1700, "high").when(F.col("amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
    .orderBy("amount_bucket", F.col("card_amount").desc())
)
