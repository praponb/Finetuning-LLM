from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 7, 9, 10]) & (F.col("quantity") > 154) & (F.year("last_restock") >= 2023))
    .orderBy("product_id")
)
