from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 9, 10]))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.avg_quantity") >= 140)
    .select(
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("pt.quantity_count"),
        F.col("pt.avg_quantity"),
    )
    .orderBy(F.col("supplier_id").desc())
)
