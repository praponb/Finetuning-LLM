from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(~F.col("p.is_active"))
    .select("src.id", "src.customer_id", "src.plan", "src.end_date", F.col("p.name"))
)
