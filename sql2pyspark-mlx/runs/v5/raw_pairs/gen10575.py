from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Bottle%") & F.col("cost").between(300, 720))
    .groupBy(
        F.when(F.col("cost") >= 620, "high").when(F.col("cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
    .filter(F.col("home_count") < 40)
    .orderBy("pct_initech")
)
