from pyspark.sql import functions as F

recent_products = df_products.filter(F.length("product_name") > 5)

agg = (
    recent_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    agg
    .select("launch_quarter", "min_stock_qty")
    .orderBy("min_stock_qty")
)
