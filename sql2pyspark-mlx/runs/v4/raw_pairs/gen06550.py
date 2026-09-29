from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 8, 10]))
    .select("id", "quantity")
    .orderBy(F.col("id").desc())
)
