from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isin([19, 28]) & F.col("product_name").isNotNull() & F.col("category").isin(["Home", "Fashion", "Sports"]))
)

products_stats = (
    base
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.countDistinct("cost").alias("unique_brands"),
    )
)

result = (
    products_stats
    .filter(F.col("unique_brands") > 10)
    .select("category", "latest_launch_date")
    .orderBy("category", F.col("latest_launch_date").desc())
    .limit(5)
)
