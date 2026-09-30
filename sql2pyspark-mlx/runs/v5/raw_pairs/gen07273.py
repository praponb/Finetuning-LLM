from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.max(F.col("o.order_date")).alias("last_order_date"),
    )
    .filter(F.col("avg_discount") > 47)
)
