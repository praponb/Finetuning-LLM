from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%UNN%"))
    .groupBy("category", "brand")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .filter(F.col("avg_price") > 44)
    .select("category", "brand", "avg_price")
)
