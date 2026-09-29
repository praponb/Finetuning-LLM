from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.col("s.end_date").between("2023-07-01", "2023-09-01"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.sort_array(F.collect_set(F.col("s.plan"))).alias("plan_list"),
    )
)
