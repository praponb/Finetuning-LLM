from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.order_date") >= "2023-11-01")
    .select(
        F.col("o.amount"),
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
)
