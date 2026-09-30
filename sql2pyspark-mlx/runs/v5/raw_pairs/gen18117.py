from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("customer_id") == 26)

summary = (
    filtered_orders
    .groupBy(
        F.year("order_date").alias("order_year"),
        F.quarter("order_date").alias("order_quarter"),
    )
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
)

result = summary.orderBy(F.col("total_shipping_fee").desc(), F.col("order_quarter").desc())
