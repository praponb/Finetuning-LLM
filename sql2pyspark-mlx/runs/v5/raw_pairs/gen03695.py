from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3200) & (F.col("product_id") == 2))
    .select("revenue")
    .orderBy(F.col("revenue").desc())
)
