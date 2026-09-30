from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "status",
        "start_date",
        F.date_add("start_date", 37).alias("start_date_plus_37d"),
    )
    .orderBy("end_date", "id")
    .limit(50)
)
