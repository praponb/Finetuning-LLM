from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.min("discount").alias("min_discount"),
    )
    .orderBy("first_order_date", F.col("store_id").desc())
)
