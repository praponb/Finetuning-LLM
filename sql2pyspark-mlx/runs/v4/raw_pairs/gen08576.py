from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").isNotNull())
    .groupBy(F.col("c.country"))
    .agg(F.sort_array(F.collect_set(F.col("s.plan"))).alias("plan_list"))
    .orderBy(F.col("country").desc())
)
