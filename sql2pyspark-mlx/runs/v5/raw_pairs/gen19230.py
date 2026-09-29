from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 6]) & ((F.col("product_id") == 5) | F.month("last_restock").isin([6, 7, 10]))))
    .select(
        "id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)
