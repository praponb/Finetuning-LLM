from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.monthly_fee").between(90, 140))
    .groupBy(
        F.col("c.is_active"),
        F.when(F.col("c.age") >= 72, "high").when(F.col("c.age") >= 35, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"))
    .orderBy("age_bucket")
)
