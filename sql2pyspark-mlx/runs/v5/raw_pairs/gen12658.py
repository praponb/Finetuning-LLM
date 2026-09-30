from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("quantity") < 3)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("oibp.stddev_unit_price"),
        F.col("oibp.max_quantity"),
    )
)
