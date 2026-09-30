from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") >= 160)
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("O"), F.col("p.id") == F.col("O.product_id"), "inner")
    .filter(F.col("O.unique_products") > 4)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("O.unique_products"),
    )
    .orderBy(F.col("price").desc(), F.col("cost").desc())
)
