from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.brand"),
        F.col("product_totals.cnt"),
        F.col("product_totals.stddev_quantity"),
    )
)
