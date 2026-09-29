from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"))
    .filter(F.col("avg_shipping_fee") >= 59)
    .orderBy("avg_shipping_fee")
)
