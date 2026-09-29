from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.price") > 900)
    .groupBy(
        F.when(F.col("p.category").isin(["Electronics", "Books"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"))
)
