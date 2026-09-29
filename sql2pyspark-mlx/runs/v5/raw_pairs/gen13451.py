from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_count") > 6)
    .select(
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("t.stddev_quantity"),
        F.col("t.total_count"),
    )
    .orderBy("launch_date")
)
