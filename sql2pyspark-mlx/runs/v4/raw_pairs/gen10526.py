from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", "product_id")
    .orderBy(F.col("last_restock").desc(), "product_id")
)
