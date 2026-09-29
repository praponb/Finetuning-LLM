from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.channel") == "web")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("c.country"),
        F.weekofyear(F.col("o.order_date")).alias("order_date_weekofyear"),
    )
)
