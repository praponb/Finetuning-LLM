from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("unique_products") >= 9)
    .select("product_id", "unique_products", "quantity_count")
    .orderBy(F.col("unique_products").desc())
)
