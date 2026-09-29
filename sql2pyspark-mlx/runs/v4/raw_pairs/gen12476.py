from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.brand"), F.col("p.price"), F.col("P.stddev_units"))
)
