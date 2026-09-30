from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNull())
    .select("last_restock", "store_id", "product_id")
)
