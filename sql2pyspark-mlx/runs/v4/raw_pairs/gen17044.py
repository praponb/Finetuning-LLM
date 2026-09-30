from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("product_id") == 1) & (F.col("quantity") < 85) & F.col("product_id").isin([3, 7])))
    .select(
        "id",
        "last_restock",
        "product_id",
        "store_id",
        F.floor("quantity").alias("quantity_whole"),
    )
    .orderBy("id")
    .limit(50)
)
