from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("age") > 64).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.plan").isin(["basic", "standard"]))
    .select(F.col("s.id"), F.col("s.status"))
    .orderBy("id")
)
