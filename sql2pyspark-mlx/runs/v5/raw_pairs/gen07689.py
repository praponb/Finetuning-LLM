from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.quantity_range") > 27)
    .select(F.col("p.cost"), F.col("t.total_unit_price"), F.col("t.quantity_range"))
)
