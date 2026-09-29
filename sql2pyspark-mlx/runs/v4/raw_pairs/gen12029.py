from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("ibp.quantity_range"),
        F.col("ibp.avg_quantity"),
    )
    .orderBy("quantity_range")
)
