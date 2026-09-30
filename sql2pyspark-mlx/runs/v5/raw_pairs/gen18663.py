from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_revenue") > 1600)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("t.avg_revenue"),
    )
    .orderBy(F.col("supplier_id").desc())
)
