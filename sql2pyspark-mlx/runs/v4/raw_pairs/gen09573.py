from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select("price", F.lit("product").alias("source"))
    .union(df_orders.select("shipping_fee", F.lit("order").alias("source")))
    .distinct()
    .orderBy("price")
)
