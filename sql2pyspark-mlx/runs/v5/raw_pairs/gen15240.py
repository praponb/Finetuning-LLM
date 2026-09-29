from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.region") == "North") | (F.upper(F.trim(F.col("s.store_name"))) == "RIVERSIDE"))
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
    )
)
