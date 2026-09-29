from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([4, 5, 9]))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
)
