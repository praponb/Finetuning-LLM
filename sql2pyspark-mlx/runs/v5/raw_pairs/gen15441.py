from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.email").like("%@outlook.com"))
    .select(
        F.col("o.order_date"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
    .orderBy("order_date")
)
