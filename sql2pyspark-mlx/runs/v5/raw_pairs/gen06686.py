from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("stores.sqft") >= 12638)
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.round(F.avg(F.col("orders.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.avg(F.col("orders.amount")).alias("avg_amount"),
    )
    .orderBy("opened_month")
    .limit(5)
)
