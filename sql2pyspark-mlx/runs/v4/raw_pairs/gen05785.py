from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") <= "2025-08-23")
    .select(
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.coalesce(F.col("s.sqft"), F.lit(0)).alias("sqft_filled"),
    )
)
