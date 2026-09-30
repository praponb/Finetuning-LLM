from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "plan", "start_date")
    .orderBy("plan")
)
