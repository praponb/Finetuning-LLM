from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("cost") >= 380)

products_stats = (
    recent_products
    .groupBy("category")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)

result = (
    products_stats
    .filter(F.col("pct_grocery") > 45)
    .orderBy("category")
    .limit(50)
)
