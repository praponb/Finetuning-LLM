from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & F.col("product_id").isin([5, 9]))
    .select(
        "id",
        "last_restock",
        "store_id",
        "quantity",
        F.date_add("last_restock", 3).alias("last_restock_plus_3d"),
    )
)
