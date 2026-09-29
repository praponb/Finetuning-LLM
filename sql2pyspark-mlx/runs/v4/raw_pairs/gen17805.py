from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 6]) & (F.col("quantity") < 62))
    .select("store_id", "quantity")
)
