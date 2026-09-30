from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .rollup(F.col("d.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.unit_price")).alias("min_unit_price"),
        F.round(F.avg(F.col("src.quantity")), 2).alias("avg_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("brand").asc_nulls_first())
)
