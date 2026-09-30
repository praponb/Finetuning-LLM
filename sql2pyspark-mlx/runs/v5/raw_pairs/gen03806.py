from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.discount") >= 0.36)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
    .orderBy("sqft")
)
