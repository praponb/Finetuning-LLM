from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([4, 9, 10]) | (F.col("o.amount") <= 540))
    .select(F.col("o.order_date"), F.col("c.city"), F.col("s.region"))
)
