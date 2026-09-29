from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("revenue") > 1800)
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.unique_products") >= 9)
    .select(F.col("p.id"), F.col("p.category"), F.col("ps.unique_products"))
    .orderBy(F.col("id").desc())
)
