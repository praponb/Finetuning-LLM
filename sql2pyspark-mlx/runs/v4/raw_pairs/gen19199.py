from pyspark.sql import functions as F

src = df_products.filter(F.year("launch_date") >= 2025)

products_stats = (
    src
    .groupBy(F.year("launch_date").alias("launch_year"), "brand")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    products_stats
    .select("launch_year", "brand", "avg_stock_qty")
    .orderBy("brand")
)
