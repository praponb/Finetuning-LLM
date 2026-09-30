from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2023-09-01") & (F.year("end_date") >= 2024))
    .select("status", "monthly_fee")
)
