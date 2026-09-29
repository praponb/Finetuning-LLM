from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2025-07-01") & (F.col("product_id").isin([1, 5, 10]) | F.col("last_restock").between("2023-08-01", "2025-08-01")))
    .orderBy("id", "last_restock")
)
