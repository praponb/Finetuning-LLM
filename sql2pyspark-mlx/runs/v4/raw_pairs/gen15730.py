from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 36]) | F.col("order_id").isin([25, 39]))
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.sum("unit_price").alias("total_unit_price"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy("unique_orders")
)
