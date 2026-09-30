from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.count("price").alias("price_count"))
    .filter(F.col("price_count") < 10)
    .select("category", "price_count")
    .orderBy("category")
    .limit(5)
)
