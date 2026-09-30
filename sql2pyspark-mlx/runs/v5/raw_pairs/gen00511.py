from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("num_orders"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
    )
)
