from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.store_name").like("Air%") & (F.col("s.sqft") != 5961))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
)
