from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.floor("amount").alias("amount_whole"),
    )
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .cube(F.col("d.city").alias("city"), F.col("s.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.shipping_fee")).alias("total_shipping_fee"),
        F.grouping_id().alias("gid"),
    )
)
