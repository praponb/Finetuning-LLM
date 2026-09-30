from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("channel").isin(["partner", "store", "web"])).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.credit_limit").between(7800, 12500))
    .select(F.col("c.id"), F.col("c.age"))
    .orderBy(F.col("id").desc())
)
