from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.month(F.col("s.opened_date")).isin([2, 10, 11]))
    .groupBy(
        F.year(F.col("s.opened_date")).alias("opened_year"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.min(F.col("o.order_date")).alias("min_order_date"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
    )
    .orderBy(
        F.col("opened_month").desc(),
        F.col("min_order_date").desc(),
        "opened_year",
    )
    .limit(20)
)
