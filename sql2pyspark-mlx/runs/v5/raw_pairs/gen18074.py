from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
    .filter(F.col("max_unit_price") > 350)
    .select("order_id", "product_id", "max_unit_price")
    .orderBy(F.col("product_id").desc(), "order_id")
    .limit(10)
)
