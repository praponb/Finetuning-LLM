from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["brand", "category"], ["brand"], []],
        F.col("dim.brand").alias("brand"),
        F.col("dim.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.quantity")).alias("min_quantity"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("category")
)
