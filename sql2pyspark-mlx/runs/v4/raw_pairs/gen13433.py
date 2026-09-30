from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("supplier_id") == 37) & (F.col("brand") != "Acme")) | F.array_contains("tags", "bestseller"))
    .select("id", "brand")
)
