from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([8, 10, 12]) & F.col("order_date").between("2023-04-01", "2024-07-21") & (F.col("customer_id") == 11))
    .groupBy(
        F.when(F.col("shipping_fee") >= 38, "high").when(F.col("shipping_fee") >= 16, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.count("*").alias("total_count"),
    )
)
