from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2025-09-01") & (F.col("quantity") < 159) & F.col("quantity").between(211, 216))
    .select("id", "product_id")
)
