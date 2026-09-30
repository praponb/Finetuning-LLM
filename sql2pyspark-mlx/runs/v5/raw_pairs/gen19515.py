from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Tokyo", "London"]))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
)
