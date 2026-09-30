from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("supplier_id").isin([5, 15, 17, 36]) & (F.col("brand") != "Umbrella") & F.array_contains("tags", "premium"))
    .withColumn("price_rounded", F.round("price", 0))
)

products_stats = (
    valid_products
    .groupBy("category")
    .agg(F.count("*").alias("num_products"))
)

result = products_stats
