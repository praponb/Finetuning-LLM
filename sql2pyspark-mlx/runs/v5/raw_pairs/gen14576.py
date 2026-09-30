from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.brand").alias("brand"), F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.quantity")).alias("min_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)
