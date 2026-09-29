from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() | (F.col("unit_price") < 340))
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)
