from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.quantity")).alias("avg_quantity"),
    )
)
