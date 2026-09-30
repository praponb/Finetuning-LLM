from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.country") != "UK")
    .groupBy(
        F.quarter(F.col("stores.opened_date")).alias("opened_quarter"),
        F.col("stores.region"),
    )
    .agg(
        F.round(F.avg(F.col("orders.discount")), 2).alias("avg_discount"),
        F.max(F.col("orders.order_date")).alias("last_order_date"),
    )
    .orderBy(F.col("opened_quarter").desc())
)
