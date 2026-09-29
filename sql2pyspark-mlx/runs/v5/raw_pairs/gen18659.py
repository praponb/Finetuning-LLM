from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.category").alias("category"), F.col("dim.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("max_quantity")
)
