from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 10, 12]))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .filter(F.col("ibp.stddev_quantity") > 35)
    .select(
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("ibp.stddev_quantity"),
        F.col("ibp.last_last_restock"),
    )
)
