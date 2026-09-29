from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.region").isin(["East", "West"]))
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
    .orderBy(F.col("sqft").desc())
)
