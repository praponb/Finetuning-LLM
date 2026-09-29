from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.city").isin(["Singapore", "New York"]) & (F.year(F.col("s.opened_date")) >= 2025))
    .select(
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("c.age"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
    .orderBy("sqft")
)
