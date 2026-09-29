from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("city").isin(["Bangkok", "Chiang Mai", "Singapore"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.customer_id"))
    .orderBy("id")
)
