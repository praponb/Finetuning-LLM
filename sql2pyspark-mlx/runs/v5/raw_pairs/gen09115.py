from pyspark.sql import functions as F

base = df_orders.filter(F.col("status").isin(["SHIPPED", "CANCELLED", "RETURNED"]))

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_customers"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
)

result = (
    status_summary
    .select("status", "unique_customers", "shipping_fee_count")
    .orderBy(F.col("shipping_fee_count").desc())
)
