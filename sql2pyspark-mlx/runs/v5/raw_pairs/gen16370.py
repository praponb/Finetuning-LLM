from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id") == 21)
    .withColumn("category_title", F.initcap(F.trim("category")))
)

products_stats = (
    base
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "supplier_id")
    .agg(F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"))
)

result = (
    products_stats
    .select("launch_quarter", "supplier_id", "home_count")
    .orderBy("home_count")
)
