from pyspark.sql import functions as F

src = df_orders.filter((F.col("channel") != "store") & (F.col("shipping_fee") <= 40))

summary = (
    src
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
)

result = summary.orderBy(F.col("avg_shipping_fee").desc(), "channel")
