from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"),
        F.sum(F.when(F.col("o.channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .orderBy("shipping_fee_count")
)
