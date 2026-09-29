from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)
