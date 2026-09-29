from pyspark.sql import functions as F

base = df_products.filter(F.col("product_name").isNotNull())

products_stats = (
    base
    .groupBy(
        "brand",
        F.when(F.col("cost") >= 700, "high").when(F.col("cost") >= 250, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    products_stats
    .filter(F.col("avg_price") > 890)
    .select("brand", "cost_bucket", "cost_count", "avg_price")
    .orderBy(F.col("cost_bucket").desc(), F.col("cost_count").desc())
)
