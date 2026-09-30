from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.stddev_quantity") > 45)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("t.stddev_quantity"),
    )
    .orderBy("id", "category")
)
