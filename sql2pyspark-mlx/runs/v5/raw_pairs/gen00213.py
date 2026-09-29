from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "partner"]))
    .groupBy("customer_id")
    .agg(F.min("discount").alias("min_discount"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "inner")
    .filter(F.col("obc.min_discount") >= 0.21)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("obc.min_discount"),
    )
    .orderBy("city")
)
