from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("order_id").isin([9, 20]))
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(
        F.col("p.category"),
        F.col("oibp.max_unit_price"),
        F.col("oibp.num_order_items"),
    )
    .orderBy(F.col("max_unit_price").desc(), F.col("num_order_items").desc())
)
