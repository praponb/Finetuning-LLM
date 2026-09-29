from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.unit_price")).alias("avg_unit_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
