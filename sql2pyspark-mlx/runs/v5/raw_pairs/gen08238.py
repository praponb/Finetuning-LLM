from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("src")
    .join(df_products.alias("dim"), F.col("src.product_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("dim.category").alias("category"),
        F.col("dim.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
        F.sum(F.col("src.unit_price")).alias("total_unit_price"),
    )
)
