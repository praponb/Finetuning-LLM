from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.col("oi.product_id").isin([25, 36]))
    .groupBy(F.col("p.category"), F.col("p.supplier_id"))
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
)
