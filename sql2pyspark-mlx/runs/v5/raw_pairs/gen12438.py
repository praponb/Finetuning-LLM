from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("stock_qty").isNotNull())

summary = (
    recent_products
    .groupBy("brand")
    .agg(F.max("price").alias("max_price"))
)

result = summary.filter(F.col("max_price") < 890)
