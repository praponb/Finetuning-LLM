from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 110)
    .select(
        "id",
        "start_date",
        "plan",
        "end_date",
        F.concat_ws("-", "plan", "status").alias("plan_status_key"),
        F.weekofyear("start_date").alias("start_date_weekofyear"),
    )
    .orderBy(F.col("id").desc(), "end_date")
    .limit(5)
)
