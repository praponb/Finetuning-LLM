from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total_2"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("e.line_total_2")), 2).alias("total_line_total_2"),
        F.avg(F.col("e.unit_price")).alias("avg_unit_price"),
    )
)
