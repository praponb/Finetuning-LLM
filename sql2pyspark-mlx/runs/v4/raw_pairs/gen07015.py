from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount") <= 1090)
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
    .orderBy(F.col("stddev_cost").desc())
)
