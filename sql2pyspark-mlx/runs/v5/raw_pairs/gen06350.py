from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.quantity")), 2).alias("total_quantity"),
    )
)
