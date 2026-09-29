from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") != "New York")
    .select(
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.round((F.col("o.discount") / F.col("o.amount")), 2).alias("discount_per_amount"),
    )
    .orderBy("city", "store_name")
)
