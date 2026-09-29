from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.city") != "Berlin")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy("status", "id")
    .limit(10)
)
