from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.store_name").like("M%"))
    .groupBy(F.col("s.region"))
    .agg(
        F.max(F.col("o.order_date")).alias("latest_order_date"),
        F.min(F.col("o.order_date")).alias("first_order_date"),
    )
)
