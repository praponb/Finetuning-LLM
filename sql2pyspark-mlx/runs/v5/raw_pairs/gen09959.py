from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.length("name") < 12).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.status"))
)
