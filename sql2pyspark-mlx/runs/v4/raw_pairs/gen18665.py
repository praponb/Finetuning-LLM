from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.discount").between(0.11, 0.15) & F.col("s.store_name").like("Ai%"))
    .select(F.col("o.discount"), F.col("s.region"), F.col("c.city"), F.col("c.age"))
)
