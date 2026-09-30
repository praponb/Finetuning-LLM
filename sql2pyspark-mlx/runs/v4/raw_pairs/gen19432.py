from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("name").like("%Ivy%")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.start_date"), F.col("s.plan"), F.col("s.status"))
    .orderBy("plan")
)
