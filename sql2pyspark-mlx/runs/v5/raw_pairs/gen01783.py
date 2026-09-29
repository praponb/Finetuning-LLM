from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("brand") == "Umbrella")

products_stats = (
    recent_products
    .groupBy("category", "brand")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
)

result = (
    products_stats
    .filter(F.col("price_range") < 31)
    .select("category", "brand", "price_range")
)
