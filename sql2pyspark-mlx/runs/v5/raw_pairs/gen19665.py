from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"))
    .orderBy(F.col("signup_year").desc(), "avg_shipping_fee")
    .limit(20)
)
