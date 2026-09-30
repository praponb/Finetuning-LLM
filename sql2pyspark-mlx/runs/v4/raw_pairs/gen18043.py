from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.quantity_range") > 50)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("oibp.quantity_range"),
        F.col("oibp.max_unit_price"),
    )
)
