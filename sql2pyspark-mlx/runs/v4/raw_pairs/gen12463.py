from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.discount") < 0.35) & F.col("s.region").isin(["East", "Central", "South"]))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
