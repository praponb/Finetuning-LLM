from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.order_date").between("2024-02-01", "2024-11-07"))
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.segment"),
    )
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
        F.max(F.col("s.delivered_date")).alias("latest_delivered_date"),
    )
)
