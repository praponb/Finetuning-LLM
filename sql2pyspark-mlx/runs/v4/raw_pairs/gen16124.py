from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") == 11530)
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        (F.col("o.discount") + F.col("o.shipping_fee")).alias("discount_plus_shipping_fee"),
    )
)
