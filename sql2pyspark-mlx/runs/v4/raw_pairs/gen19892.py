from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1670, "high").when(F.col("amount") >= 580, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(F.min("order_date").alias("min_order_date"))
)
