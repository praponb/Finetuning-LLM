from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 540)
    .groupBy(F.when(F.col("price") >= 1090, "high").when(F.col("price") >= 170, "mid").otherwise("low").alias("price_bucket"))
    .pivot("category", ["Electronics", "Books", "Grocery", "Sports"])
    .agg(F.max("stock_qty"))
)
