from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("unique_products")
)
