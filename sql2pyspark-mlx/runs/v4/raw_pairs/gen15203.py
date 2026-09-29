from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").isNull())
    .groupBy(F.col("c.country"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_statuss"))
    .orderBy(F.col("country").desc(), "unique_statuss")
)
