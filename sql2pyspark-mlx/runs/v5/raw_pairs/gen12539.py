from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 17)
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("cnt") >= 10)
    .orderBy("last_order_date")
)
