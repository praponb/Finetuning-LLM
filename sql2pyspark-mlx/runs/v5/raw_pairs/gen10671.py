from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("o.status"),
        F.col("o.amount"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
