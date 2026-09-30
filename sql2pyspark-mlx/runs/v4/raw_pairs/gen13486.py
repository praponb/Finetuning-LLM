from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.city").isin(["Berlin", "Singapore", "Tokyo"]))
    .select(
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("order_date", "region")
)
