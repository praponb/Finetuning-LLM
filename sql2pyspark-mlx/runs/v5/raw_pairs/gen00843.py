from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
    .orderBy(F.col("shipping_fee").desc(), "id")
)
