from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4400, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 6)
)
