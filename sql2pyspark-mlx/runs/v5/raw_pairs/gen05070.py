from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.store_sale_count") > 6)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("ps.store_sale_count"),
        F.col("ps.first_sale_date"),
    )
    .orderBy("store_sale_count")
)
