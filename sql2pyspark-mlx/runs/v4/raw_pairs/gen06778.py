from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 165) | (F.col("store_id") == 1))
    .select(
        "last_restock",
        "product_id",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
    )
    .orderBy(F.col("quantity_adjusted").desc(), F.col("last_restock").desc())
)
