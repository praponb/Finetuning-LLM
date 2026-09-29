from pyspark.sql import functions as F

src = df_products.filter(F.lower("product_name").like("%ate%"))

products_stats = (
    src
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    products_stats
    .filter(F.col("max_cost") < 250)
    .select("category", "last_launch_date")
    .orderBy("category")
)
