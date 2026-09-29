from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.year("order_date") >= 2025)
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.countDistinct("amount").alias("unique_stores"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.stddev_shipping_fee") > 52)
    .select(
        F.col("c.email"),
        F.col("c.city"),
        F.col("cs.stddev_shipping_fee"),
        F.col("cs.unique_stores"),
    )
    .orderBy(F.col("email").desc(), "unique_stores")
)
