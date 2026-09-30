from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "basic", "enterprise"]) & (F.year("end_date") == 2024))
    .select("id", "end_date", "start_date")
)
