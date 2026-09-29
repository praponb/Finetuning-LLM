from pyspark.sql import functions as F

result = (
    df_products
    .select("stock_qty", "tags")
    .orderBy(F.col("stock_qty").desc())
)
