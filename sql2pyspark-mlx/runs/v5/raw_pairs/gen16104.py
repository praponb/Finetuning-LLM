from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower("product_name").like("%sb-%") & (F.col("stock_qty") <= 356))
    .select("id", "brand", (F.col("cost") * F.col("price")).alias("margin"))
)
