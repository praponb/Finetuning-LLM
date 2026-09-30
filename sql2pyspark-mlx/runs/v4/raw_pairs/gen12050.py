from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | F.col("unit_price").between(200, 330))
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") < 37)
    .orderBy("unique_orders")
)
