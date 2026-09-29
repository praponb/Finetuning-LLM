from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(F.col("o.order_date"), F.col("c.credit_limit"), F.col("s.opened_date"))
)
