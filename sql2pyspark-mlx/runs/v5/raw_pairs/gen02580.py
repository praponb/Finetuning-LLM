from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNotNull())
    .select("category", "brand")
    .distinct()
)
