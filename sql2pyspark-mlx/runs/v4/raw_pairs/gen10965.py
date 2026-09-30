from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([8, 9, 10]) & F.col("o.channel").isin(["mobile", "store", "partner"]))
    .select(
        F.col("s.sqft"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
)
