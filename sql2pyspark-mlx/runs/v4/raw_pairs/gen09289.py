from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 177) & F.col("store_id").isin([2, 3, 4]) & (F.col("product_id") == 3))
    .select("id", "product_id")
    .orderBy("id")
)
