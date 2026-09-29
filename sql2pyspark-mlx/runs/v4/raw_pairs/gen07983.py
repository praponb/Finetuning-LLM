from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.country") == "TH")
    .groupBy(F.col("customers.id"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.countDistinct(F.col("orders.shipping_fee")).alias("unique_channels"),
        F.min(F.col("orders.order_date")).alias("first_order_date"),
    )
)
