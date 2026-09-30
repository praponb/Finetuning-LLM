from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.country"),
    )
    .agg(
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
    )
    .orderBy(F.col("max_shipping_fee").desc())
)
