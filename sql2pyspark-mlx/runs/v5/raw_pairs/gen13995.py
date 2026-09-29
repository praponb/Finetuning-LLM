from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "quantity",
        "last_restock",
        "product_id",
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
    .orderBy("id", F.col("last_restock").desc())
    .limit(50)
)
