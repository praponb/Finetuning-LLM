from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.greatest(F.col("o.amount"), F.col("o.shipping_fee")).alias("greatest_amount_shipping_fee"),
    )
)
