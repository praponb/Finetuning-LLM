from pyspark.sql import functions as F

result = (
    df_products
    .select("product_name", "cost")
    .orderBy(F.col("product_name").desc(), F.col("cost").desc())
)
