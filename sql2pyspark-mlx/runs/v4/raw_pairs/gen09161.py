from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_count") >= 4)
    .select(F.col("p.brand"), F.col("p.supplier_id"), F.col("t.total_count"))
)
