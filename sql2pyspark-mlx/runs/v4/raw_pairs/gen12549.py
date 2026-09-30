from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("discount") <= 0.25) | F.col("discount").between(0.24, 0.33))
    .withColumn("discount_level", F.when(F.col("discount") >= 0.36, "High").when(F.col("discount") >= 0.06, "Medium").otherwise("Low"))
)

order_quarter_summary = (
    valid_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
    )
)

result = (
    order_quarter_summary
    .filter(F.col("completed_shipping_fee") > 56)
    .orderBy(F.col("completed_shipping_fee").desc())
)
