from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.city").isin(["Berlin", "Tokyo"]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
