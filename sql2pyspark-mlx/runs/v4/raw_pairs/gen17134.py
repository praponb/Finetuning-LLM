from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("channel", "status")
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .orderBy(F.col("status").desc())
)
