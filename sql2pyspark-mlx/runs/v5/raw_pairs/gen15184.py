from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("cost").between(410, 420))
    .withColumn("brand_lower", F.lower("brand"))
)

summary = (
    recent_products
    .groupBy("category")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = (
    summary
    .orderBy(F.col("stock_qty_count").desc(), "category")
    .limit(5)
)
