from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity").between(218, 223))
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.quantity_range") > 61)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("pt.quantity_range"),
    )
)
