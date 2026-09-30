from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "New York")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.year(F.col("s.opened_date")).alias("opened_year"),
    )
    .agg(
        F.max(F.col("o.order_date")).alias("latest_order_date"),
        F.sum(F.col("o.shipping_fee")).alias("total_shipping_fee"),
    )
    .orderBy("opened_month", "latest_order_date")
)
