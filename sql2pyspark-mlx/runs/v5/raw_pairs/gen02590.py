from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("kind"))
    .union(
        df_order_items
        .filter(F.col("order_id") == 27)
        .select("unit_price", F.lit("order_item").alias("kind"))
    )
    .distinct()
)
