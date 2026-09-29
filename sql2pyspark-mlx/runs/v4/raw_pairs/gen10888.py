from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(F.col("unit_price").alias("value"))
    .subtract(df_shipments.select(F.col("cost").alias("value")))
)
