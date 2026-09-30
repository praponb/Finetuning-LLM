from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.opened_date") < "2023-10-01")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .filter(F.col("pending_count") > 69)
)
