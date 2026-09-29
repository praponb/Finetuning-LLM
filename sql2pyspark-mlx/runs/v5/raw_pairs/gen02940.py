from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.date_trunc("WEEK", F.col("o.order_date")).alias("order_date_period"),
    )
)
