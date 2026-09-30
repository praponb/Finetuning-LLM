from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("quantity") < 176)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.floor("quantity").alias("quantity_whole"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"))
)
