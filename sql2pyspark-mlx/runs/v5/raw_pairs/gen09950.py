from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .select(F.lit("order_item").alias("kind"), F.col("unit_price").alias("value"))
    .unionByName(
        df_store_sales
        .select(F.lit("store_sale").alias("kind"), F.col("revenue").alias("value"))
    )
)
