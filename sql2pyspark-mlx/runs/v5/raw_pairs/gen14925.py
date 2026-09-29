from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.lit("order_item").alias("kind"), "unit_price")
    .unionByName(
        df_products
        .filter(F.size("tags") > 1)
        .select(F.lit("product").alias("kind"), F.col("price").alias("unit_price"))
    )
    .distinct()
    .orderBy("kind")
)
