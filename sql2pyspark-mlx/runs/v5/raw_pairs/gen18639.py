from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"))
    .intersect(df_order_items.select(F.col("unit_price").alias("value")))
    .orderBy("value")
)
