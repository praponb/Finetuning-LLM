from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.email"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.sum(F.col("orders.shipping_fee")).alias("total_shipping_fee"),
    )
    .orderBy("id")
)
