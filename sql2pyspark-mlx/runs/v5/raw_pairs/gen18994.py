from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 8]) & (F.col("quantity") < 144))
    .select("id", "quantity", "last_restock")
    .orderBy("last_restock")
)
