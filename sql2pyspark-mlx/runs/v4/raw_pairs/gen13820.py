from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.month("order_date").alias("order_month"),
        F.quarter("order_date").alias("order_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") < 24)
    .orderBy(F.col("order_quarter").desc(), "order_month")
)
