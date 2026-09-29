from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("is_active")).select("id")

result = (
    df_orders.alias("orders")
    .join(excluded_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("orders.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("orders.amount"),
    )
)
