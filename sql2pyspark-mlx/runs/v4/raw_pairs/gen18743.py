from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.store_name").isNotNull())
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("c.city"),
        (F.col("o.discount") - F.col("o.amount")).alias("discount_minus_amount"),
    )
    .orderBy(F.col("order_date").desc())
)
