from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .filter(F.col("ibp.unique_stores") > 9)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("ibp.unique_stores"),
        F.col("ibp.max_quantity"),
    )
    .orderBy(F.col("id").desc(), "unique_stores")
)
