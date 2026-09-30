from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("segment").isin(["SMB", "Corporate", "Consumer"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(~F.col("s.plan").isin(["basic", "enterprise", "premium"]))
    .select(F.col("s.id"), F.col("s.plan"), F.col("s.end_date"))
    .orderBy("id")
)
