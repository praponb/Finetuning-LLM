from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") > "2025-09-06")
    .withColumn("category_brand", F.concat("category", F.lit(" "), "brand"))
    .withColumn("product_name_prefix", F.substring("product_name", 1, 4))
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([27, 39])), "left_semi")
)
