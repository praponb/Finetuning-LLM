from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        (F.col("o.amount") / F.nullif(F.col("o.shipping_fee"), F.lit(0))).alias("amount_per_shipping_fee"),
    )
)
