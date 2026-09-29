from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.opened_date") > "2024-10-01")
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
)
