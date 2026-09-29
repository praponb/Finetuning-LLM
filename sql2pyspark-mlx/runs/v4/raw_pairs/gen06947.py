from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.dayofweek("end_date").alias("end_date_dayofweek"),
    )
)
