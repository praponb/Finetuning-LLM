from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .filter(F.col("ssbp.min_revenue") > 2600)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ssbp.avg_units"),
        F.col("ssbp.min_revenue"),
    )
    .orderBy("avg_units")
)
