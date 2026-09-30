from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 2) & (F.col("status").isin(["SHIPPED", "CANCELLED", "PENDING"]) | F.col("customer_id").isin([12, 19, 37])))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
    .orderBy(F.col("returned_count").desc())
)
