from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1750, "high").when(F.col("amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 850)
    .drop("avg_amount")
)
