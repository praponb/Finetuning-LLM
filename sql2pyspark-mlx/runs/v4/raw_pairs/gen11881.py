from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["CANCELLED", "RETURNED"]) & F.col("store_id").isNotNull())
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy(F.col("order_month").desc())
    .limit(5)
)
