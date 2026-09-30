from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5]))
    .select(
        "*",
        F.when(F.col("quantity") >= 220, "High").when(F.col("quantity") >= 100, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.last_restock",
        F.col("s.store_name"),
    )
)
