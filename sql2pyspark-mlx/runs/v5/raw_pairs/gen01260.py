from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2023-08-01") & (F.col("start_date") > "2024-04-01"))
)
