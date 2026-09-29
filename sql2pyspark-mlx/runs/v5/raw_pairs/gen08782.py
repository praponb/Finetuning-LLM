from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard"]))
    .select("id", "plan", "start_date", "status")
)
