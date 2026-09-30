from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([8, 11, 31, 37]))
    .select(F.col("unit_price").alias("value"))
    .unionByName(df_orders.select(F.col("amount").alias("value")))
    .orderBy("value")
)
