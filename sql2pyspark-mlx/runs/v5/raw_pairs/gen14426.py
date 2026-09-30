from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .select("id", "plan", "customer_id", "start_date")
)
