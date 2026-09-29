from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("cost").alias("value"), F.lit("product").alias("kind"))
    .unionByName(
        df_order_items
        .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("kind"))
    )
)
