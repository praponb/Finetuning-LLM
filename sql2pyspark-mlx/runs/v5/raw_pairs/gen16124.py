from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .cube(F.col("d.country").alias("country"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.discount")).alias("max_discount"),
        F.max(F.col("s.shipping_fee")).alias("max_shipping_fee"),
    )
    .orderBy(F.col("max_shipping_fee").asc_nulls_first())
)
