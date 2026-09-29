from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("price").alias("value"))
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select(F.col("unit_price").alias("value"))
    )
    .distinct()
    .orderBy("value")
)
