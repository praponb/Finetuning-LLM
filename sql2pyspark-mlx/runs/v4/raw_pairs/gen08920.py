from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isNotNull())
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("c.segment"),
    )
)
