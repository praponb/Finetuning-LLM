from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit").isNotNull())
    .groupBy(
        F.when(F.col("o.status").isin(["CANCELLED", "SHIPPED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.max(F.col("s.cost")).alias("max_cost"))
)
