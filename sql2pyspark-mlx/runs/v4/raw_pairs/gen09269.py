from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select("id", "product_id", F.round("unit_price", 0).alias("unit_price_rounded"))
)
