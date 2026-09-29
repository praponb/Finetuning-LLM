from pyspark.sql import functions as F

src = df_orders.filter(F.col("customer_id").isNull() & (F.col("channel") == "mobile"))

summary = (
    src
    .groupBy("customer_id")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)

result = (
    summary
    .select("customer_id", "discount_range")
    .orderBy(F.col("customer_id").desc())
)
