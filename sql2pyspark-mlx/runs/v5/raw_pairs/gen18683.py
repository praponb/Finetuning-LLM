from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([2, 3, 5]))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("c.name"),
        F.col("c.credit_limit"),
        (F.col("o.discount") * F.col("o.shipping_fee")).alias("discount_times_shipping_fee"),
    )
)
