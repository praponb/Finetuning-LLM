from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "product_id",
        "last_restock",
        "store_id",
        F.date_add("last_restock", 88).alias("last_restock_plus_88d"),
    )
    .orderBy("last_restock_plus_88d", F.col("product_id").desc())
)
