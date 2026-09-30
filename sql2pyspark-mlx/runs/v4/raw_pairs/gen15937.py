from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 30) & F.col("amount").between(440, 1240))
    .groupBy(
        F.when(F.col("amount") >= 1570, "high").when(F.col("amount") >= 510, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
