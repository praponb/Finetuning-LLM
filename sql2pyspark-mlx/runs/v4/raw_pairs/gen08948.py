from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "product_name")
    .orderBy(F.col("id").desc())
    .limit(3)
)
