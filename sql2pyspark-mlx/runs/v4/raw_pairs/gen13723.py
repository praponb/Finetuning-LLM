from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("customer_id") == 37) & (F.col("status") == "CANCELLED")))
    .groupBy(
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.min("amount").alias("min_amount"))
)
