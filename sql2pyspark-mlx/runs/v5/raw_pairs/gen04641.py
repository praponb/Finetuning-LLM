from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 110)
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy(F.col("order_id").desc())
    .limit(50)
)
