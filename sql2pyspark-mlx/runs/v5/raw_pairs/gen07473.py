from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("brand").desc())
)
