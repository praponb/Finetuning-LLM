from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") >= 2024) | (F.col("monthly_fee") < 70))
    .select("start_date")
    .orderBy("start_date")
)
