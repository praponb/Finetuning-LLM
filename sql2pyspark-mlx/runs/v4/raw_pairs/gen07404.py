from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("start_date") < "2023-05-27") | F.month("end_date").isin([3, 7, 8])))
    .select("id", "start_date", "end_date", "status")
)
