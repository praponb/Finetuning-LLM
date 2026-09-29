from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7, 9]))
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.countDistinct("units").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.stock_qty"), F.col("t.units_count"), F.col("t.unique_stores"))
    .orderBy("stock_qty")
)
