from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 195) | F.month("last_restock").isin([4, 9, 11]))
    .select("store_id", "quantity", "product_id")
)
