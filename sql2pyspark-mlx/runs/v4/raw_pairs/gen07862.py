from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("monthly_fee") < 180).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.credit_limit"))
    .orderBy(F.col("name").desc())
)
