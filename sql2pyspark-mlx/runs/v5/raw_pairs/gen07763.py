from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock").between("2024-02-01", "2024-11-16"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

result = (
    valid
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([2, 4]))
        .select("product_id", "id")
    )
)
