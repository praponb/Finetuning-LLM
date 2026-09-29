from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity") <= 211)
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("t.unique_stores"),
    )
)
