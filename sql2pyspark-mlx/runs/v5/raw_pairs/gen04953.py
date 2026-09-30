from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.quantity")), 2).alias("total_quantity"),
    )
)
