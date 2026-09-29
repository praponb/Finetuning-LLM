from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 2300)
    .orderBy(F.col("product_id").desc())
)
