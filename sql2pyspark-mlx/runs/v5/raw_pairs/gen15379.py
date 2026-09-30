from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.unit_price")).alias("min_unit_price"),
    )
    .orderBy(F.col("rows_total").desc())
)
