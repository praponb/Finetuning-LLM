from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 297)
    .select("id", "category")
)
