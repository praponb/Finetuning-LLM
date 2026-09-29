from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.region"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(F.max(F.col("o.order_date")).alias("last_order_date"))
    .orderBy("last_order_date")
)
