from pyspark.sql import functions as F

src = df_products.filter(F.col("supplier_id").isin([9, 12, 22]))

agg = (
    src
    .groupBy("category")
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)

result = (
    agg
    .filter(F.col("stock_qty_range") > 70)
    .select("category", "stock_qty_range")
    .orderBy("category")
)
