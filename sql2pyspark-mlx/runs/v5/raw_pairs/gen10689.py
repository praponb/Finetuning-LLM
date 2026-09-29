from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.length("email") >= 7).select("id")

result = (
    df_orders.alias("o")
    .join(excluded_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
    )
    .orderBy("order_date")
)
