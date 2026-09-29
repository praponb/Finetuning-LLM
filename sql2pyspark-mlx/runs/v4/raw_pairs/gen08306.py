from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
    .orderBy("pct_store", "order_quarter")
    .limit(3)
)
