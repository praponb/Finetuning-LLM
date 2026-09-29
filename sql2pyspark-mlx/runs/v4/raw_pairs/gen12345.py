from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").between(125, 239) & (F.col("quantity") < 130)) | F.col("store_id").isin([1, 3, 5]))
    .select("last_restock")
    .orderBy(F.col("last_restock").desc())
)
