from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("product_id") == 26) & (F.col("quantity") >= 7)))
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("unit_price").alias("unique_orders"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("total_quantity") > 22)
)
