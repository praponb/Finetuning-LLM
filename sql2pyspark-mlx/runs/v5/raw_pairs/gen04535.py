from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "plan",
        "end_date",
        F.substring("plan", 1, 4).alias("plan_prefix"),
        F.last_day("end_date").alias("end_date_month_end"),
    )
)
