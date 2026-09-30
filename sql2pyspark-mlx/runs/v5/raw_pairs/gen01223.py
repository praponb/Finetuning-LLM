from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(89, 208))
    .select("quantity", "store_id", "product_id")
    .orderBy(F.col("product_id").desc())
)
