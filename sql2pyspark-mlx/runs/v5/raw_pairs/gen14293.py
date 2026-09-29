from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 14)
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("latest_order_date"),
    )
    .orderBy(F.col("first_order_date").desc())
)
