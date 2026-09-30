from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("amount") >= 480) & ((F.col("shipping_fee") <= 15) | ~F.col("status").isin(["CANCELLED", "PENDING", "COMPLETED"]))))
    .groupBy("channel")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
