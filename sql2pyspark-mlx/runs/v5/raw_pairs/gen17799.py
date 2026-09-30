from pyspark.sql import functions as F

base = (
    df_products
    .filter(~((F.col("launch_date") > "2025-01-01") | F.col("supplier_id").isNotNull()))
    .withColumn("cost_whole", F.ceil("cost"))
)

brand_summary = (
    base
    .groupBy("brand", "category")
    .agg(F.count("*").alias("num_products"))
)

result = (
    brand_summary
    .filter(F.col("num_products") >= 3)
    .select("brand", "category", "num_products")
    .orderBy(F.col("brand").desc(), F.col("num_products").desc())
)
