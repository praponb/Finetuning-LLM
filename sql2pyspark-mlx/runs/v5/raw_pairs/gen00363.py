from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.quantity_count") > 4)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("t.quantity_count"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(10)
)
