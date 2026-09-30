from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.lit("order_item").alias("origin"), "unit_price")
    .unionByName(
        df_store_sales
        .select(F.lit("store_sale").alias("origin"), F.col("revenue").alias("unit_price"))
    )
    .distinct()
)
