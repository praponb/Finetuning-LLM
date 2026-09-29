from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("category").isin(["Sports", "Fashion", "Books"]))

agg = (
    valid_products
    .groupBy("supplier_id")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    agg
    .filter(F.col("min_stock_qty") < 160)
    .select("supplier_id", "min_stock_qty")
    .orderBy(F.col("min_stock_qty").desc())
)
