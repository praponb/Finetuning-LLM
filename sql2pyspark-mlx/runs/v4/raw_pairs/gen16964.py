from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        F.date_format("start_date", "yyyy-MM").alias("start_date_label"),
    )
)
