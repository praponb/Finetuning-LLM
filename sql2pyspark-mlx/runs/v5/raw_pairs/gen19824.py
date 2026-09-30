from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 110) | (F.col("end_date") <= "2024-11-01"))
    .select("plan", "end_date")
)
