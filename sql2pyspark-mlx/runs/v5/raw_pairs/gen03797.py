from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 2100)
    .orderBy("order_id")
    .limit(20)
)
