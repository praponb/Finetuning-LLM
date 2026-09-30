from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy(
        F.when(F.col("discount") >= 0.39, "high").when(F.col("discount") >= 0.13, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("amount").alias("max_amount"),
    )
)
